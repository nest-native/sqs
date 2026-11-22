import { SqsReceivedMessage, SqsMessageAttributeValue } from './driver';
import { decodeSqsAttributes, SqsAttributeValue } from './sqs-message-codec';

/** Where a message came from: the logical key and the resolved queue. */
export interface SqsQueueInfo {
  /** The logical key handlers and the producer use. */
  readonly key: string;
  /** The queue's name. */
  readonly name: string;
  /** The queue's URL. */
  readonly url: string;
  /** Whether the queue is FIFO (its name ends in `.fifo`). */
  readonly isFifo: boolean;
  /** The configured client that talks to it. */
  readonly client: string;
}

/**
 * The per-message controls the dispatcher hands to a context.
 *
 * @internal
 */
export interface SqsMessageControl {
  delete(): Promise<void>;
  changeVisibility(seconds: number): Promise<void>;
}

/**
 * The transport context of one received message, injected with `@SqsCtx()`.
 * It exposes SQS as it is — the raw message, its receive count, its
 * attributes — and the two manual controls SQS offers on a received message.
 *
 * @publicApi
 */
export class SqsContext {
  constructor(
    private readonly queue: SqsQueueInfo,
    private readonly message: SqsReceivedMessage,
    /**
     * Aborts when the transport gives up on this message: at the end of the
     * shutdown drain. Pass it to the I/O your handler does.
     */
    readonly signal: AbortSignal,
    private readonly control: SqsMessageControl,
  ) {}

  /** The queue the message came from. */
  getQueue(): SqsQueueInfo {
    return this.queue;
  }

  /** The message as SQS returned it (the SDK's `Message` shape). */
  getMessage(): SqsReceivedMessage {
    return this.message;
  }

  getMessageId(): string {
    return this.message.MessageId ?? '';
  }

  /** The exact body text, before any parsing. */
  getBody(): string {
    return this.message.Body ?? '';
  }

  /** Your message attributes, decoded: text for `String`/`Number`, bytes for `Binary`. */
  getAttributes(): Record<string, SqsAttributeValue> {
    return decodeSqsAttributes(this.message.MessageAttributes);
  }

  /** Your message attributes as SQS returned them. */
  getMessageAttributes(): Record<string, SqsMessageAttributeValue> {
    return this.message.MessageAttributes ?? {};
  }

  /** The system attributes: `ApproximateReceiveCount`, `SentTimestamp`, … */
  getSystemAttributes(): Partial<Record<string, string>> {
    return this.message.Attributes ?? {};
  }

  /** How many times SQS has delivered this message, this delivery included. */
  getReceiveCount(): number {
    const count = Number(this.message.Attributes?.ApproximateReceiveCount);
    return Number.isInteger(count) && count > 0 ? count : 1;
  }

  /** The message group: a FIFO group, or a fair-queue tenant on a standard queue. */
  getGroupId(): string | undefined {
    return this.message.Attributes?.MessageGroupId;
  }

  /**
   * Delete the message now. The transport then does not delete it again when
   * the handler returns, and an error thrown afterwards cannot retry it.
   */
  delete(): Promise<void> {
    return this.control.delete();
  }

  /**
   * Change the message's visibility timeout, counted from now (0 to 43 200
   * seconds; the 12-hour budget from the receive still applies).
   */
  changeVisibility(seconds: number): Promise<void> {
    return this.control.changeVisibility(seconds);
  }
}
