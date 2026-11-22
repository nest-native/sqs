import { LoggerService } from '@nestjs/common';
import { settlesWithin, SqsClock } from './clock';
import { SQS_MAX_VISIBILITY_SECONDS } from './constants';
import { SqsReceivedMessage } from './driver';
import { SqsContext, SqsMessageControl, SqsQueueInfo } from './sqs-context';
import { SqsErrorBehavior, SqsErrorMapper } from './sqs-error-mapping';
import { remainingVisibilitySeconds, SqsHeartbeat, SqsHeartbeatBeat } from './sqs-heartbeat';
import { parseSqsBody } from './sqs-message-codec';
import { SqsPermits } from './sqs-permits';
import { retryDelaySeconds, SqsRetryBackoffOptions } from './sqs-retry-backoff';
import { SqsSettler } from './sqs-settler';

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
  /** Extends the visibility of every held message; unset when it is off. */
  heartbeat?: SqsHeartbeat;
  random?: () => number;
}

interface Delivery {
  readonly message: SqsReceivedMessage;
  readonly receiptHandle: string;
  readonly receivedAt: number;
  readonly abort: AbortController;
  /** FIFO queues only: the message group whose chain the message is in. */
  readonly group?: string;
  /** Its visibility heartbeat, from receive to settle; unset when the heartbeat is off. */
  readonly beat?: SqsHeartbeatBeat;
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
 * On a FIFO queue, the messages of one group run one at a time, in receive
 * order, each after its predecessor settled; different groups run
 * concurrently. The messages waiting behind a running one stay held, each with
 * its permit and its heartbeat. A `'retry'` holds the group: once the failed
 * message's new visibility is set, the group's later held messages are
 * released (visibility 0), so SQS redelivers the failed message first. A
 * delete SQS refuses does the same, since the message is still in the queue.
 *
 * @internal
 */
export class SqsDispatcher {
  /** Every held message: waiting in a group chain, running, or being settled. */
  private readonly held = new Set<Delivery>();
  /** The messages whose handler started, until they are settled. */
  private readonly running = new Map<Delivery, Promise<void>>();
  /** FIFO queues only: each group's held messages in receive order; the first one runs. */
  private readonly chains = new Map<string, Delivery[]>();

  constructor(private readonly options: SqsDispatcherOptions) {}

  /** How many messages are held. */
  get size(): number {
    return this.held.size;
  }

  /**
   * Take one received message. It owns one permit, given back once it is
   * settled. On a FIFO queue it runs once every earlier held message of its
   * group has settled.
   */
  dispatch(message: SqsReceivedMessage): void {
    const receiptHandle = message.ReceiptHandle as string;
    const receivedAt = this.options.clock.now();
    const delivery: Delivery = {
      message,
      receiptHandle,
      receivedAt,
      abort: new AbortController(),
      group: this.options.queue.isFifo ? (message.Attributes?.MessageGroupId ?? '') : undefined,
      beat: this.options.heartbeat?.start({ messageId: message.MessageId ?? '', receiptHandle, receivedAt }),
      settled: false,
      abandoned: false,
    };
    this.held.add(delivery);
    if (delivery.group === undefined) {
      this.start(delivery);
      return;
    }
    const chain = this.chains.get(delivery.group);
    if (chain) {
      chain.push(delivery);
      return;
    }
    this.chains.set(delivery.group, [delivery]);
    this.start(delivery);
  }

  /**
   * Release every held message whose handler has not started — on a FIFO
   * queue, the messages waiting behind a running one of their group — with
   * visibility 0, so another consumer can take them at once. The first step of
   * the shutdown drain.
   */
  async releaseWaiting(): Promise<void> {
    const waiting = [...this.chains.values()].flatMap(chain => chain.splice(1));
    if (waiting.length === 0) {
      return;
    }
    this.options.logger.log(
      `Releasing ${waiting.length} message(s) on "${this.options.queue.key}" that were waiting behind an ` +
        'earlier message of their group.',
    );
    await this.release(waiting);
  }

  /**
   * Wait up to `timeoutMs` for the messages being handled. Those still running
   * then are abandoned: their `signal` aborts and they are released
   * (visibility 0), so another consumer picks them up at once.
   */
  async drain(timeoutMs: number): Promise<void> {
    const pending = [...this.running.values()];
    if (pending.length === 0) {
      return;
    }
    if (await settlesWithin(this.options.clock, Promise.allSettled(pending), timeoutMs)) {
      return;
    }
    // A message whose delete or retry is already under way is finishing, not stuck.
    const stragglers = [...this.running.keys()].filter(delivery => !delivery.settled);
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
    }
    await Promise.all(stragglers.map(delivery => this.setVisibility(delivery, 0)));
  }

  private start(delivery: Delivery): void {
    const work = this.process(delivery).finally(() => this.complete(delivery));
    this.running.set(delivery, work);
  }

  /** A running message settled: give its permit back and run the next of its group. */
  private complete(delivery: Delivery): void {
    this.running.delete(delivery);
    this.held.delete(delivery);
    this.options.permits.give(1);
    if (delivery.group === undefined) {
      return;
    }
    const chain = this.chains.get(delivery.group) as Delivery[];
    chain.shift();
    const next = chain[0];
    if (next) {
      this.start(next);
    } else {
      this.chains.delete(delivery.group);
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
      await this.releaseSuccessors(delivery);
    } else {
      await delivery.beat?.stop();
      if (!(await this.options.settler.delete(delivery.receiptHandle))) {
        // SQS still has the message and will deliver it again: nothing of its group may run first.
        await this.releaseSuccessors(delivery);
      }
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

  /** Set the backoff as the message's visibility; without a backoff, leave it alone. */
  private async retry(delivery: Delivery, receiveCount: number): Promise<void> {
    await delivery.beat?.stop();
    const backoff = this.options.retryBackoff;
    if (!backoff) {
      return;
    }
    const remaining = remainingVisibilitySeconds(this.options.clock, delivery.receivedAt);
    if (remaining < 1) {
      return;
    }
    const delay = retryDelaySeconds(backoff, receiveCount, this.options.random);
    await this.options.settler.changeVisibility(delivery.receiptHandle, Math.min(delay, remaining));
  }

  /**
   * FIFO queues only: release the messages held behind a retried one in its
   * group, or behind one whose delete SQS refused, so none of them runs before
   * SQS redelivers it. Called once the message's visibility is set or its
   * delete answered: SQS then holds the group until that message is visible
   * again, and serves it first.
   */
  private async releaseSuccessors(delivery: Delivery): Promise<void> {
    if (delivery.group === undefined) {
      return;
    }
    // The chain exists while its first message runs; the retried message is that one.
    const successors = (this.chains.get(delivery.group) as Delivery[]).splice(1);
    if (successors.length > 0) {
      await this.release(successors);
    }
  }

  /** Release held messages that never ran (visibility 0) and give their permits back. */
  private async release(deliveries: Delivery[]): Promise<void> {
    await Promise.all(
      deliveries.map(async delivery => {
        await this.setVisibility(delivery, 0);
        this.held.delete(delivery);
        this.options.permits.give(1);
      }),
    );
  }

  /** Stop the message's heartbeat, then set its visibility. */
  private async setVisibility(delivery: Delivery, seconds: number): Promise<boolean> {
    await delivery.beat?.stop();
    return this.options.settler.changeVisibility(delivery.receiptHandle, seconds);
  }

  private control(delivery: Delivery): SqsMessageControl {
    return {
      delete: async () => {
        if (delivery.settled || delivery.abandoned) {
          return;
        }
        delivery.settled = true;
        await delivery.beat?.stop();
        if (!(await this.options.settler.delete(delivery.receiptHandle))) {
          await this.releaseSuccessors(delivery);
          throw new Error(`SQS did not delete message ${delivery.message.MessageId}.`);
        }
      },
      changeVisibility: async (seconds: number) => {
        if (!Number.isInteger(seconds) || seconds < 0 || seconds > SQS_MAX_VISIBILITY_SECONDS) {
          throw new RangeError(`A visibility timeout is an integer from 0 to ${SQS_MAX_VISIBILITY_SECONDS} seconds.`);
        }
        if (!(await this.setVisibility(delivery, seconds))) {
          throw new Error(`SQS did not change the visibility of message ${delivery.message.MessageId}.`);
        }
      },
    };
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
