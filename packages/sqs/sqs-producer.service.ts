import { Inject, Injectable } from '@nestjs/common';
import { SQS_MAX_BATCH_ENTRIES, SQS_MAX_MESSAGE_BYTES } from './constants';
import { SqsBatchFailure, SqsSendEntry } from './driver';
import { SqsModuleOptions } from './interfaces';
import { SqsQueueInfo } from './sqs-context';
import { encodeSqsAttributes, serializeSqsBody, SqsAttributeInput } from './sqs-message-codec';
import { SqsMessageValidationError, sqsMessageBytes, validateSqsSendEntry } from './sqs-message-validation';
import { ResolvedSqsQueue, SqsQueueRegistry } from './sqs-queue-registry';
import { SQS_MODULE_OPTIONS } from './tokens';

/**
 * A message to send.
 *
 * @publicApi
 */
export interface SqsOutgoingMessage<T = unknown> {
  /** A string is sent as is; anything else is sent as JSON. */
  body: T;
  /** Up to 10 message attributes. */
  attributes?: Record<string, SqsAttributeInput>;
  /**
   * `MessageGroupId`: required on a FIFO queue (the ordering group); on a
   * standard queue it opts the message into fair queueing. Never set for you.
   */
  groupId?: string;
  /** `MessageDeduplicationId`, FIFO queues only. */
  deduplicationId?: string;
  /** `DelaySeconds`, 0 to 900, standard queues only. */
  delaySeconds?: number;
  /** The X-Ray trace header, sent as the `AWSTraceHeader` system attribute. */
  traceHeader?: string;
}

/** Per-call options. */
export interface SqsSendOptions {
  /** Aborts the request. */
  signal?: AbortSignal;
}

/** A message SQS accepted. */
export interface SqsSendResult {
  messageId: string;
  /** FIFO queues only. */
  sequenceNumber?: string;
}

/**
 * SQS refused a message `send()` sent.
 *
 * @publicApi
 */
export class SqsSendError extends Error {
  override readonly name = 'SqsSendError';

  constructor(
    readonly queue: string,
    readonly code: string,
    readonly senderFault: boolean,
    detail?: string,
  ) {
    super(`SQS refused the message for "${queue}": ${code}${detail ? ` (${detail})` : ''}.`);
  }
}

/** One message of a batch that SQS accepted, by its index in the batch. */
export interface SqsBatchSendSuccess extends SqsSendResult {
  index: number;
}

/** One message of a batch that SQS refused, by its index in the batch. */
export interface SqsBatchSendFailure {
  index: number;
  code: string;
  message?: string;
  senderFault: boolean;
}

/**
 * Not every message of a `sendBatch()` was accepted. Each message is in
 * exactly one list:
 *
 * - `successful` — SQS accepted it;
 * - `failed` — SQS refused it, so it was not enqueued;
 * - `uncertain` — the request carrying it failed (a network error, a
 *   throttle): it may or may not have been enqueued;
 * - `notAttempted` — it was never sent: on a FIFO queue the batch stops at the
 *   first failure, so no message overtakes an earlier one, and any batch stops
 *   at a failed request.
 *
 * @publicApi
 */
export class SqsBatchSendError extends Error {
  override readonly name = 'SqsBatchSendError';

  constructor(
    readonly queue: string,
    readonly successful: SqsBatchSendSuccess[],
    readonly failed: SqsBatchSendFailure[],
    readonly uncertain: number[],
    readonly notAttempted: number[],
    cause?: unknown,
  ) {
    super(
      `${successful.length} of ${
        successful.length + failed.length + uncertain.length + notAttempted.length
      } messages for "${queue}" were accepted (${failed.length} refused, ${uncertain.length} uncertain, ` +
        `${notAttempted.length} not attempted).`,
    );
    (this as { cause?: unknown }).cause = cause;
  }
}

interface PreparedEntry {
  index: number;
  entry: SqsSendEntry;
  bytes: number;
}

interface BatchProgress {
  successful: SqsBatchSendSuccess[];
  failed: SqsBatchSendFailure[];
  uncertain: number[];
  notAttempted: number[];
  cause?: unknown;
}

/**
 * Sends messages to SQS.
 *
 * Queues are logical keys, as in `@SqsHandler`: no producer has to be
 * registered before it sends. Every message is checked against SQS's rules
 * before it leaves — body characters, size, attributes, FIFO fields — so a
 * doomed message fails with the rule named. `send()` sends one message;
 * `sendBatch()` splits any number into requests of at most 10 messages and
 * 1 MiB, and reports every message's fate.
 *
 * @publicApi
 */
@Injectable()
export class SqsProducerService {
  private readonly maxMessageBytes: number;

  constructor(
    @Inject(SQS_MODULE_OPTIONS) options: SqsModuleOptions,
    private readonly registry: SqsQueueRegistry,
  ) {
    const maxMessageBytes = options.producer?.maxMessageBytes ?? SQS_MAX_MESSAGE_BYTES;
    if (!Number.isInteger(maxMessageBytes) || maxMessageBytes < 1 || maxMessageBytes > SQS_MAX_MESSAGE_BYTES) {
      throw new Error(`producer.maxMessageBytes must be an integer from 1 to ${SQS_MAX_MESSAGE_BYTES}.`);
    }
    this.maxMessageBytes = maxMessageBytes;
  }

  /** Resolve a logical queue key to the queue it names. */
  async resolveQueue(queue: string): Promise<SqsQueueInfo> {
    const { key, name, url, isFifo, client } = await this.registry.resolve(queue);
    return { key, name, url, isFifo, client };
  }

  /** Send one message. Rejects with {@link SqsSendError} when SQS refuses it. */
  async send<T>(queue: string, message: SqsOutgoingMessage<T>, options: SqsSendOptions = {}): Promise<SqsSendResult> {
    const resolved = await this.registry.resolve(queue);
    const { entry } = this.prepare(resolved, message, 0, 'The message');
    const outcome = await resolved.driver.sendMessages(resolved.url, [entry], options);
    const [failure] = outcome.failed;
    if (failure) {
      throw new SqsSendError(resolved.key, failure.code, failure.senderFault, failure.message);
    }
    const [success] = outcome.successful;
    if (!success) {
      throw new SqsSendError(resolved.key, 'NoResult', false, 'SQS reported neither success nor failure');
    }
    return toResult(success);
  }

  /**
   * Send many messages, in requests of at most 10 messages and 1 MiB, one
   * request at a time. Resolves with every result, in order, when SQS accepted
   * every message; rejects with {@link SqsBatchSendError} otherwise. Every
   * message is validated before the first request, so an invalid one sends
   * nothing.
   */
  async sendBatch<T>(
    queue: string,
    messages: SqsOutgoingMessage<T>[],
    options: SqsSendOptions = {},
  ): Promise<SqsSendResult[]> {
    if (messages.length === 0) {
      return [];
    }
    const resolved = await this.registry.resolve(queue);
    const prepared = messages.map((message, index) => this.prepare(resolved, message, index));
    const progress: BatchProgress = { successful: [], failed: [], uncertain: [], notAttempted: [] };
    const chunks = chunk(prepared);
    for (let position = 0; position < chunks.length; position += 1) {
      const proceed = await this.sendChunk(resolved, chunks[position], options, progress);
      if (!proceed) {
        progress.notAttempted = chunks.slice(position + 1).flatMap(rest => rest.map(item => item.index));
        break;
      }
    }
    if (progress.successful.length < messages.length) {
      throw new SqsBatchSendError(
        resolved.key,
        progress.successful,
        progress.failed,
        progress.uncertain,
        progress.notAttempted,
        progress.cause,
      );
    }
    return progress.successful
      .sort((left, right) => left.index - right.index)
      .map(({ index: _index, ...result }) => result);
  }

  /** Send one request. Resolves whether the batch should go on. */
  private async sendChunk(
    queue: ResolvedSqsQueue,
    items: PreparedEntry[],
    options: SqsSendOptions,
    progress: BatchProgress,
  ): Promise<boolean> {
    let outcome: Awaited<ReturnType<ResolvedSqsQueue['driver']['sendMessages']>>;
    try {
      outcome = await queue.driver.sendMessages(
        queue.url,
        items.map(item => item.entry),
        options,
      );
    } catch (error) {
      progress.uncertain.push(...items.map(item => item.index));
      progress.cause = error;
      return false;
    }
    for (const success of outcome.successful) {
      progress.successful.push({ index: Number(success.id), ...toResult(success) });
    }
    progress.failed.push(...outcome.failed.map(toBatchFailure));
    return outcome.failed.length === 0 || !queue.isFifo;
  }

  private prepare<T>(
    queue: ResolvedSqsQueue,
    message: SqsOutgoingMessage<T>,
    index: number,
    label = `Message ${index}`,
  ): PreparedEntry {
    let entry: SqsSendEntry;
    try {
      entry = {
        id: String(index),
        body: serializeSqsBody(message.body),
        delaySeconds: message.delaySeconds,
        messageAttributes: message.attributes ? encodeSqsAttributes(message.attributes) : undefined,
        traceHeader: message.traceHeader,
        messageGroupId: message.groupId,
        messageDeduplicationId: message.deduplicationId,
      };
      validateSqsSendEntry(entry, { fifo: queue.isFifo, maxMessageBytes: this.maxMessageBytes });
    } catch (error) {
      throw new SqsMessageValidationError(
        `${label} for "${queue.key}": ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    return { index, entry, bytes: sqsMessageBytes(entry.body, entry.messageAttributes) };
  }
}

/** Split entries into requests of at most 10 entries and 1 MiB. */
function chunk(entries: PreparedEntry[]): PreparedEntry[][] {
  const chunks: PreparedEntry[][] = [];
  let current: PreparedEntry[] = [];
  let bytes = 0;
  for (const item of entries) {
    if (current.length === SQS_MAX_BATCH_ENTRIES || bytes + item.bytes > SQS_MAX_MESSAGE_BYTES) {
      chunks.push(current);
      current = [];
      bytes = 0;
    }
    current.push(item);
    bytes += item.bytes;
  }
  chunks.push(current);
  return chunks;
}

function toResult(success: { messageId: string; sequenceNumber?: string }): SqsSendResult {
  return success.sequenceNumber === undefined
    ? { messageId: success.messageId }
    : { messageId: success.messageId, sequenceNumber: success.sequenceNumber };
}

function toBatchFailure(failure: SqsBatchFailure): SqsBatchSendFailure {
  const base = { index: Number(failure.id), code: failure.code, senderFault: failure.senderFault };
  return failure.message === undefined ? base : { ...base, message: failure.message };
}
