import { LoggerService } from '@nestjs/common';
import { SqsClock } from './clock';
import { SQS_MAX_VISIBILITY_SECONDS } from './constants';
import { SqsSettler } from './sqs-settler';

/**
 * Seconds kept in reserve below the 12-hour budget, for the time between the
 * receive SQS counts from and the moment the consumer saw the message.
 */
const VISIBILITY_BUDGET_MARGIN_SECONDS = 30;

/**
 * What is left, in whole seconds, of the 12 hours SQS allows a message to stay
 * invisible after `receivedAt`: "Extending the timeout doesn't reset this
 * 12-hour limit", and SQS refuses a visibility timeout beyond what is left
 * instead of shortening it.
 *
 * @internal
 */
export function remainingVisibilitySeconds(clock: SqsClock, receivedAt: number): number {
  const elapsed = Math.ceil((clock.now() - receivedAt) / 1000);
  return SQS_MAX_VISIBILITY_SECONDS - elapsed - VISIBILITY_BUDGET_MARGIN_SECONDS;
}

/**
 * The visibility heartbeat's timing, resolved against the consumer's
 * visibility timeout.
 *
 * @internal
 */
export interface ResolvedSqsHeartbeat {
  /** Seconds between two extensions of one held message. */
  intervalSeconds: number;
  /** The visibility timeout each extension sets, counted from the extension. */
  extendBySeconds: number;
}

/** @internal */
export interface SqsHeartbeatRuntimeOptions extends ResolvedSqsHeartbeat {
  settler: SqsSettler;
  clock: SqsClock;
  logger: LoggerService;
  queueKey: string;
}

/** The message a heartbeat keeps invisible. @internal */
export interface SqsHeldMessage {
  messageId: string;
  receiptHandle: string;
  /** When the consumer received it, on the consumer's clock. */
  receivedAt: number;
}

/**
 * The heartbeat of one held message.
 *
 * @internal
 */
export interface SqsHeartbeatBeat {
  /**
   * Stop extending the message. Resolves once every extension already sent
   * has been answered, so a delete or a visibility change sent afterwards
   * reaches SQS after the last extension, never before it.
   */
  stop(): Promise<void>;
}

/**
 * Keeps the messages a consumer holds invisible while they wait or run: every
 * `intervalSeconds` from its receive until it is settled, a held message gets
 * a new visibility timeout of `extendBySeconds` (one `ChangeMessageVisibility`
 * entry, batched with the consumer's other visibility changes).
 *
 * Each extension is clamped to what is left of the 12 hours SQS allows from
 * the receive. The extension that reaches that limit is the last one: the
 * heartbeat stops and warns, and the message becomes visible again when the
 * limit is reached.
 *
 * @internal
 */
export class SqsHeartbeat {
  constructor(private readonly options: SqsHeartbeatRuntimeOptions) {}

  /** Start extending a held message, `intervalSeconds` after its receive. */
  start(message: SqsHeldMessage): SqsHeartbeatBeat {
    return new MessageHeartbeat(this.options, message);
  }
}

class MessageHeartbeat implements SqsHeartbeatBeat {
  private timer: unknown;
  private stopped = false;
  /** Every extension sent so far, answered or not. */
  private sent: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly options: SqsHeartbeatRuntimeOptions,
    private readonly message: SqsHeldMessage,
  ) {
    this.schedule();
  }

  stop(): Promise<void> {
    if (!this.stopped) {
      this.stopped = true;
      this.options.clock.clearTimeout(this.timer);
    }
    return this.sent.then(() => undefined);
  }

  private schedule(): void {
    this.timer = this.options.clock.setTimeout(() => this.beat(), this.options.intervalSeconds * 1000);
  }

  private beat(): void {
    const { extendBySeconds, clock } = this.options;
    const remaining = remainingVisibilitySeconds(clock, this.message.receivedAt);
    if (remaining >= extendBySeconds) {
      this.extend(extendBySeconds);
      this.schedule();
      return;
    }
    this.stopped = true;
    if (remaining >= 1) {
      this.extend(remaining);
    }
    this.options.logger.warn(
      `Message ${this.message.messageId} on "${this.options.queueKey}" has nearly used the 12 hours SQS ` +
        'allows a message to stay invisible after a receive; its visibility is not extended again, and it ' +
        'becomes visible to consumers when its current visibility timeout ends.',
    );
  }

  private extend(seconds: number): void {
    const extension = this.options.settler.changeVisibility(this.message.receiptHandle, seconds);
    this.sent = Promise.all([this.sent, extension]);
  }
}
