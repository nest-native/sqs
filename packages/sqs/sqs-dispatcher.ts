import { LoggerService } from '@nestjs/common';
import { settlesWithin, SqsClock } from './clock';
import { SQS_MAX_VISIBILITY_SECONDS } from './constants';
import { SqsReceivedMessage } from './driver';
import { SqsContext, SqsMessageControl, SqsQueueInfo } from './sqs-context';
import { SqsErrorBehavior, SqsErrorMapper } from './sqs-error-mapping';
import { parseSqsBody } from './sqs-message-codec';
import { SqsPermits } from './sqs-permits';
import { retryDelaySeconds, SqsRetryBackoffOptions } from './sqs-retry-backoff';
import { SqsSettler } from './sqs-settler';

/**
 * Seconds kept in reserve below the 12-hour budget, for the time between our
 * receive and SQS's clock.
 */
const VISIBILITY_BUDGET_MARGIN_SECONDS = 30;

/**
 * What a handler runner receives for one message: the parsed body and its
 * context.
 *
 * @internal
 */
export interface SqsHandlerInvocation {
  payload: unknown;
  context: SqsContext;
}

/** @internal */
export interface SqsDispatcherOptions {
  queue: SqsQueueInfo;
  run(invocation: SqsHandlerInvocation): Promise<unknown>;
  errorMapper: SqsErrorMapper;
  retryBackoff?: Required<SqsRetryBackoffOptions>;
  settler: SqsSettler;
  permits: SqsPermits;
  clock: SqsClock;
  logger: LoggerService;
  random?: () => number;
}

interface Delivery {
  readonly message: SqsReceivedMessage;
  readonly receiptHandle: string;
  readonly receivedAt: number;
  readonly abort: AbortController;
  /** Deleted (automatically or with `ctx.delete()`) or retried: nothing more to do. */
  settled: boolean;
  /** Released at the end of the shutdown drain: its outcome is ignored. */
  abandoned: boolean;
}

/**
 * Runs each received message through its handler and settles it.
 *
 * A message is deleted only after its handler returned (or the error mapper
 * chose `'delete'`, or the handler called `ctx.delete()`). A `'retry'` sets the
 * message's visibility to the backoff for its receive count — never below one
 * second — so a failing message is not redelivered in a tight loop.
 *
 * @internal
 */
export class SqsDispatcher {
  private readonly inFlight = new Map<Delivery, Promise<void>>();

  constructor(private readonly options: SqsDispatcherOptions) {}

  get size(): number {
    return this.inFlight.size;
  }

  /** Run one message. It owns one permit, given back once it is settled. */
  dispatch(message: SqsReceivedMessage): void {
    const delivery: Delivery = {
      message,
      receiptHandle: message.ReceiptHandle as string,
      receivedAt: this.options.clock.now(),
      abort: new AbortController(),
      settled: false,
      abandoned: false,
    };
    const work = this.process(delivery).finally(() => {
      this.inFlight.delete(delivery);
      this.options.permits.give(1);
    });
    this.inFlight.set(delivery, work);
  }

  /**
   * Wait up to `timeoutMs` for the messages in flight. Those still running then
   * are abandoned: their `signal` aborts and they are released (visibility 0),
   * so another consumer picks them up at once.
   */
  async drain(timeoutMs: number): Promise<void> {
    const pending = [...this.inFlight.values()];
    if (pending.length === 0) {
      return;
    }
    if (await settlesWithin(this.options.clock, Promise.allSettled(pending), timeoutMs)) {
      return;
    }
    // A message whose delete or retry is already under way is finishing, not stuck.
    const stragglers = [...this.inFlight.keys()].filter(delivery => !delivery.settled);
    if (stragglers.length === 0) {
      return;
    }
    this.options.logger.warn(
      `${stragglers.length} message(s) on "${this.options.queue.key}" were still being handled after ` +
        `${timeoutMs} ms; releasing them for another consumer.`,
    );
    for (const delivery of stragglers) {
      delivery.abandoned = true;
      delivery.abort.abort();
      void this.options.settler.changeVisibility(delivery.receiptHandle, 0);
    }
  }

  private async process(delivery: Delivery): Promise<void> {
    const context = new SqsContext(
      this.options.queue,
      delivery.message,
      delivery.abort.signal,
      this.control(delivery),
    );
    let outcome: SqsErrorBehavior | 'handled';
    try {
      await this.options.run({ payload: parseSqsBody(delivery.message.Body), context });
      outcome = 'handled';
    } catch (error) {
      outcome = await this.map(error, context);
    }
    if (delivery.settled || delivery.abandoned) {
      return;
    }
    delivery.settled = true;
    if (outcome === 'retry') {
      await this.retry(delivery, context.getReceiveCount());
    } else {
      await this.options.settler.delete(delivery.receiptHandle);
    }
  }

  private async map(error: unknown, context: SqsContext): Promise<SqsErrorBehavior> {
    let behavior: SqsErrorBehavior;
    try {
      behavior = (await this.options.errorMapper(error, context)) === 'delete' ? 'delete' : 'retry';
    } catch (mapperError) {
      this.options.logger.error(
        `The SQS error mapper threw while mapping a failure on "${this.options.queue.key}" ` +
          `(${describe(mapperError)}); retrying the message.`,
      );
      behavior = 'retry';
    }
    this.options.logger.warn(
      `Message ${context.getMessageId()} on "${this.options.queue.key}" failed: ${describe(error)}; ` +
        `${behavior === 'delete' ? 'deleting it' : 'retrying it'}.`,
    );
    return behavior;
  }

  private async retry(delivery: Delivery, receiveCount: number): Promise<void> {
    const backoff = this.options.retryBackoff;
    if (!backoff) {
      return;
    }
    const remaining = this.remainingBudgetSeconds(delivery);
    if (remaining < 1) {
      return;
    }
    const delay = retryDelaySeconds(backoff, receiveCount, this.options.random);
    await this.options.settler.changeVisibility(delivery.receiptHandle, Math.min(delay, remaining));
  }

  /** What is left of the 12 hours SQS allows a received message, in seconds. */
  private remainingBudgetSeconds(delivery: Delivery): number {
    const elapsed = Math.ceil((this.options.clock.now() - delivery.receivedAt) / 1000);
    return SQS_MAX_VISIBILITY_SECONDS - elapsed - VISIBILITY_BUDGET_MARGIN_SECONDS;
  }

  private control(delivery: Delivery): SqsMessageControl {
    return {
      delete: async () => {
        if (delivery.settled || delivery.abandoned) {
          return;
        }
        delivery.settled = true;
        if (!(await this.options.settler.delete(delivery.receiptHandle))) {
          throw new Error(`SQS did not delete message ${delivery.message.MessageId}.`);
        }
      },
      changeVisibility: async (seconds: number) => {
        if (!Number.isInteger(seconds) || seconds < 0 || seconds > SQS_MAX_VISIBILITY_SECONDS) {
          throw new RangeError(`A visibility timeout is an integer from 0 to ${SQS_MAX_VISIBILITY_SECONDS} seconds.`);
        }
        if (!(await this.options.settler.changeVisibility(delivery.receiptHandle, seconds))) {
          throw new Error(`SQS did not change the visibility of message ${delivery.message.MessageId}.`);
        }
      },
    };
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
