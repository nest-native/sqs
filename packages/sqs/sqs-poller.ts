import { randomUUID } from 'node:crypto';
import { LoggerService } from '@nestjs/common';
import { sleep, SqsClock } from './clock';
import { SqsReceivedMessage } from './driver';
import { SqsPermits } from './sqs-permits';
import { ResolvedSqsQueue } from './sqs-queue-registry';

/** The first delay after a failed receive; it doubles per failure, up to 30 s. */
const RECEIVE_BACKOFF_INITIAL_MS = 1_000;
const RECEIVE_BACKOFF_MAX_MS = 30_000;

/** @internal */
export interface SqsPollerOptions {
  queue: ResolvedSqsQueue;
  maxMessages: number;
  waitTimeSeconds: number;
  receiveTimeoutMs: number;
  visibilityTimeout?: number;
  permits: SqsPermits;
  clock: SqsClock;
  logger: LoggerService;
  /** Hand one received message to the dispatcher; it gives its permit back. */
  dispatch(message: SqsReceivedMessage): void;
  /** Make messages received after stop visible again (visibility 0). */
  release(messages: SqsReceivedMessage[]): void;
}

/**
 * The receive loop of one consumer.
 *
 * It long-polls only when it has free permits, and asks for no more messages
 * than it has permits for. Every receive runs under a deadline, so a request
 * stuck on a half-open connection is aborted and retried instead of stalling
 * the loop: the SDK sets no request timeout of its own. A failed receive backs
 * off (1 s doubling to 30 s) and the loop goes on; it ends only when stopped.
 *
 * @internal
 */
export class SqsPoller {
  private readonly stopping = new AbortController();
  private receiving?: AbortController;
  private loop?: Promise<void>;
  private consecutiveFailures = 0;

  constructor(private readonly options: SqsPollerOptions) {}

  start(): void {
    this.loop ??= this.run();
  }

  /**
   * Stop receiving. By default the receive in flight is not aborted: stop waits
   * for it to return (it cannot outlast its deadline) and releases whatever it
   * returned, so no message is left invisible until its visibility timeout.
   * `abortReceive` aborts it instead.
   */
  async stop(abortReceive: boolean): Promise<void> {
    this.stopping.abort();
    if (abortReceive) {
      this.receiving?.abort();
    }
    await this.loop;
  }

  private async run(): Promise<void> {
    while (await this.options.permits.waitForAvailable(this.stopping.signal)) {
      await this.pollOnce();
    }
  }

  private async pollOnce(): Promise<void> {
    const { permits } = this.options;
    const requested = Math.min(this.options.maxMessages, permits.available);
    permits.take(requested);
    const outcome = await this.receiveWithDeadline(requested);
    if ('messages' in outcome) {
      this.accept(outcome.messages, requested);
      return;
    }
    permits.give(requested);
    await this.recover(outcome.failure);
  }

  /** One receive under its deadline. The deadline is cleared before any backoff starts. */
  private async receiveWithDeadline(
    maxMessages: number,
  ): Promise<{ messages: SqsReceivedMessage[] } | { failure: string }> {
    const { clock, receiveTimeoutMs } = this.options;
    const receiving = new AbortController();
    this.receiving = receiving;
    let timedOut = false;
    const deadline = clock.setTimeout(() => {
      timedOut = true;
      receiving.abort();
    }, receiveTimeoutMs);
    let outcome: { messages: SqsReceivedMessage[] } | { failure: string };
    try {
      outcome = { messages: await this.receive(maxMessages, receiving.signal) };
    } catch (error) {
      outcome = { failure: timedOut ? `no response within ${receiveTimeoutMs} ms` : describe(error) };
    }
    clock.clearTimeout(deadline);
    this.receiving = undefined;
    return outcome;
  }

  private receive(maxMessages: number, signal: AbortSignal): Promise<SqsReceivedMessage[]> {
    const { queue } = this.options;
    return queue.driver.receiveMessages(
      {
        queueUrl: queue.url,
        maxMessages,
        waitTimeSeconds: this.options.waitTimeSeconds,
        visibilityTimeout: this.options.visibilityTimeout,
        receiveRequestAttemptId: queue.isFifo ? randomUUID() : undefined,
      },
      { signal },
    );
  }

  private accept(messages: SqsReceivedMessage[], requested: number): void {
    this.consecutiveFailures = 0;
    const settleable = messages.filter(message => message.ReceiptHandle);
    if (settleable.length < messages.length) {
      this.options.logger.warn(
        `SQS returned ${messages.length - settleable.length} message(s) without a receipt handle ` +
          `on "${this.options.queue.key}"; they cannot be settled and were skipped.`,
      );
    }
    this.options.permits.give(requested - settleable.length);
    if (this.stopping.signal.aborted) {
      this.options.permits.give(settleable.length);
      if (settleable.length > 0) {
        this.options.release(settleable);
      }
      return;
    }
    for (const message of settleable) {
      this.options.dispatch(message);
    }
  }

  private async recover(reason: string): Promise<void> {
    if (this.stopping.signal.aborted) {
      return;
    }
    this.consecutiveFailures += 1;
    const delay = Math.min(
      RECEIVE_BACKOFF_MAX_MS,
      RECEIVE_BACKOFF_INITIAL_MS * 2 ** (this.consecutiveFailures - 1),
    );
    this.options.logger.warn(
      `Receiving from "${this.options.queue.key}" failed (${reason}); retrying in ${delay} ms ` +
        `(failure ${this.consecutiveFailures} in a row).`,
    );
    await sleep(this.options.clock, delay, this.stopping.signal);
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
