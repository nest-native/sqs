import { createHash, randomUUID } from 'node:crypto';
import { SqsClock } from '../clock';
import {
  SQS_MAX_BATCH_ENTRIES,
  SQS_MAX_MESSAGE_ATTRIBUTES,
  SQS_MAX_MESSAGE_BYTES,
  SQS_MAX_VISIBILITY_SECONDS,
  SQS_MAX_WAIT_TIME_SECONDS,
} from '../constants';
import {
  SqsBatchFailure,
  SqsBatchOutcome,
  SqsCallOptions,
  SqsDriver,
  SqsDriverFactory,
  SqsMessageAttributeValue,
  SqsReceiptEntry,
  SqsReceivedMessage,
  SqsReceiveRequest,
  SqsSendEntry,
  SqsSendOutcome,
  SqsVisibilityEntry,
} from '../driver';
import { encodeSqsAttributes, SqsAttributeInput } from '../sqs-message-codec';
import { containsInvalidSqsCharacter, sqsMessageBytes } from '../sqs-message-validation';
import { VirtualClock } from './virtual-clock';

/** Options for {@link InMemorySqs}. */
export interface InMemorySqsOptions {
  /** The time source. Defaults to a fresh {@link VirtualClock}. */
  clock?: SqsClock;
  /** @default 'us-east-1' */
  region?: string;
  /** @default '000000000000' */
  accountId?: string;
  /** The base of every queue URL. @default 'http://sqs.in-memory.test' */
  endpoint?: string;
  /** Create a queue on the first `GetQueueUrl` for its name. @default false */
  autoCreateQueues?: boolean;
}

/** The attributes `createQueue` accepts, as strings, as `CreateQueue` takes them. */
export interface InMemorySqsQueueAttributes {
  /** Seconds, 0 to 43 200. @default '30' */
  VisibilityTimeout?: string;
  /** Seconds, 0 to 900. @default '0' */
  DelaySeconds?: string;
  /** Bytes, 1 024 to 1 048 576. @default '1048576' */
  MaximumMessageSize?: string;
  /** `{"deadLetterTargetArn": "...", "maxReceiveCount": 3}` */
  RedrivePolicy?: string;
}

/** The operations {@link InMemorySqs.deny} can refuse. */
export type InMemorySqsOperation =
  | 'ReceiveMessage'
  | 'DeleteMessageBatch'
  | 'ChangeMessageVisibilityBatch'
  | 'SendMessageBatch'
  | 'GetQueueUrl'
  | 'GetQueueAttributes';

/** A stored message, as {@link InMemorySqs.messages} reports it. */
export interface InMemorySqsMessageState {
  messageId: string;
  body: string;
  attributes: Record<string, SqsMessageAttributeValue>;
  receiveCount: number;
  /** Visible now: a receive would return it. */
  visible: boolean;
  /** Received, and neither deleted nor given a new visibility since. */
  inFlight: boolean;
  /** Set on a message the redrive policy moved here. */
  deadLetterSourceArn?: string;
}

/**
 * An error shaped like the SDK's service errors: its `name` is the SQS error
 * code (`QueueDoesNotExist`, `AccessDenied`, …).
 *
 * @publicApi
 */
export class InMemorySqsError extends Error {
  readonly $fault = 'client';

  constructor(code: string, message: string) {
    super(message);
    this.name = code;
  }
}

interface StoredMessage {
  readonly id: string;
  readonly body: string;
  readonly md5: string;
  readonly messageAttributes: Record<string, SqsMessageAttributeValue>;
  readonly traceHeader?: string;
  readonly sentAt: number;
  visibleAt: number;
  receiveCount: number;
  firstReceiveAt?: number;
  receivedAt?: number;
  handle?: string;
  inFlight: boolean;
  deadLetterSourceArn?: string;
}

interface QueueState {
  readonly name: string;
  readonly url: string;
  readonly arn: string;
  readonly visibilityTimeout: number;
  readonly delaySeconds: number;
  readonly maximumMessageSize: number;
  readonly redrivePolicy?: string;
  readonly redrive?: { targetArn: string; maxReceiveCount: number };
  messages: StoredMessage[];
  polled: boolean;
}

const QUEUE_ATTRIBUTES = new Set([
  'VisibilityTimeout',
  'DelaySeconds',
  'MaximumMessageSize',
  'RedrivePolicy',
  'QueueArn',
  'ApproximateNumberOfMessages',
  'ApproximateNumberOfMessagesNotVisible',
  'ApproximateNumberOfMessagesDelayed',
]);

/**
 * An in-memory SQS with the semantics this package depends on, behind the same
 * driver seam as the real client:
 *
 * - a receive hides a message for its visibility timeout and issues a new
 *   receipt handle; `ApproximateReceiveCount` counts every receive;
 * - a long poll waits until a message becomes visible, the wait ends, or it is
 *   aborted;
 * - a delete with an older receipt handle succeeds but deletes nothing — the
 *   worst case AWS documents;
 * - a visibility change counts from the call, applies only to a message in
 *   flight, and cannot pass 12 hours from the receive;
 * - a redrive policy moves a message to its dead-letter queue on the receive
 *   after `maxReceiveCount`;
 * - every argument SQS refuses is refused, with SQS's error code. A test double
 *   that accepts anything proves nothing about the calls made to it.
 *
 * FIFO queues are not modeled yet, and `createQueue` refuses them.
 *
 * Time follows the clock, by default a {@link VirtualClock}: move it with
 * `sqs.clock.advance(ms)` instead of sleeping. {@link idle} waits until every
 * polled queue has nothing visible and nothing being processed.
 *
 * @publicApi
 */
export class InMemorySqs {
  readonly clock: SqsClock;
  private readonly region: string;
  private readonly accountId: string;
  private readonly endpoint: string;
  private readonly autoCreateQueues: boolean;
  private readonly queues = new Map<string, QueueState>();
  private readonly handles = new Map<string, { queue: QueueState; message: StoredMessage }>();
  private readonly denied = new Set<InMemorySqsOperation>();
  private readonly listeners = new Set<() => void>();

  constructor(options: InMemorySqsOptions = {}) {
    this.clock = options.clock ?? new VirtualClock();
    this.region = options.region ?? 'us-east-1';
    this.accountId = options.accountId ?? '000000000000';
    this.endpoint = (options.endpoint ?? 'http://sqs.in-memory.test').replace(/\/+$/, '');
    this.autoCreateQueues = options.autoCreateQueues ?? false;
  }

  /** Create a standard queue and return its URL. Creating it again returns the same URL. */
  createQueue(name: string, attributes: InMemorySqsQueueAttributes = {}): string {
    if (!/^[A-Za-z0-9_-]{1,80}$/.test(name)) {
      throw new InMemorySqsError(
        'InvalidParameterValue',
        name.endsWith('.fifo')
          ? `FIFO queues are not modeled by InMemorySqs yet: "${name}".`
          : `"${name}" is not a valid queue name.`,
      );
    }
    const existing = this.queues.get(name);
    if (existing) {
      return existing.url;
    }
    const queue = buildQueue(name, `${this.endpoint}/${this.accountId}/${name}`, this.arnOf(name), attributes);
    this.queues.set(name, queue);
    return queue.url;
  }

  /** The URL of a queue, by name. */
  queueUrl(name: string): string {
    return this.queueByName(name).url;
  }

  /** The ARN of a queue, by name. */
  queueArn(name: string): string {
    return this.queueByName(name).arn;
  }

  /** A driver for `SqsModuleOptions.driverFactory`. */
  createDriver(): SqsDriver {
    return {
      client: this,
      receiveMessages: (request, options) => this.receiveMessages(request, options),
      deleteMessages: async (queueUrl, entries) => this.deleteMessages(queueUrl, entries),
      changeVisibility: async (queueUrl, entries) => this.changeVisibility(queueUrl, entries),
      sendMessages: async (queueUrl, entries) => this.sendMessages(queueUrl, entries),
      getQueueUrl: async (queueName, ownerAccountId) => this.getQueueUrl(queueName, ownerAccountId),
      getQueueAttributes: async (queueUrl, names) => this.getQueueAttributes(queueUrl, names),
      destroy: () => undefined,
    };
  }

  /** A driver factory that hands every client a driver over this instance. */
  createDriverFactory(): SqsDriverFactory {
    return () => this.createDriver();
  }

  /**
   * Put a message on a queue (by name or URL) the way a producer would, and
   * return its id. It bypasses {@link deny}.
   */
  sendMessage(
    queue: string,
    body: string,
    options: { attributes?: Record<string, SqsAttributeInput>; delaySeconds?: number } = {},
  ): string {
    const target = this.queueByNameOrUrl(queue);
    const outcome = this.store(target, [
      {
        id: '0',
        body,
        delaySeconds: options.delaySeconds,
        messageAttributes: options.attributes ? encodeSqsAttributes(options.attributes) : undefined,
      },
    ]);
    const [failure] = outcome.failed;
    if (failure) {
      throw new InMemorySqsError(failure.code, failure.message as string);
    }
    return outcome.successful[0].messageId;
  }

  /** Every message stored on a queue (by name or URL), in send order. */
  messages(queue: string): InMemorySqsMessageState[] {
    const now = this.clock.now();
    return this.queueByNameOrUrl(queue).messages.map(message => ({
      messageId: message.id,
      body: message.body,
      attributes: message.messageAttributes,
      receiveCount: message.receiveCount,
      visible: message.visibleAt <= now,
      inFlight: message.inFlight,
      ...(message.deadLetterSourceArn ? { deadLetterSourceArn: message.deadLetterSourceArn } : {}),
    }));
  }

  /** Refuse an operation with `AccessDenied`, as a missing IAM permission would. */
  deny(operation: InMemorySqsOperation): void {
    this.denied.add(operation);
  }

  /** Allow an operation {@link deny} refused. */
  allow(operation: InMemorySqsOperation): void {
    this.denied.delete(operation);
  }

  /**
   * Resolves once every queue that has been polled has no visible message and
   * none being processed; rejects after `timeoutMs` of real time otherwise.
   *
   * "Being processed" means received and not yet deleted or given a new
   * visibility. A failure that leaves the visibility untouched
   * (`retryBackoff: false`) therefore keeps its message in flight until the
   * visibility expires and the message is received again.
   */
  idle(options: { timeoutMs?: number } = {}): Promise<void> {
    if (this.isIdle()) {
      return Promise.resolve();
    }
    const timeoutMs = options.timeoutMs ?? 5_000;
    return new Promise((resolve, reject) => {
      const check = (): void => {
        if (this.isIdle()) {
          clearTimeout(timer);
          this.listeners.delete(check);
          resolve();
        }
      };
      const timer = setTimeout(() => {
        this.listeners.delete(check);
        reject(new Error(`InMemorySqs was not idle within ${timeoutMs} ms: ${this.describeBusy()}.`));
      }, timeoutMs);
      this.listeners.add(check);
    });
  }

  private async receiveMessages(request: SqsReceiveRequest, options?: SqsCallOptions): Promise<SqsReceivedMessage[]> {
    this.authorize('ReceiveMessage');
    validateReceive(request);
    const queue = this.queueByUrl(request.queueUrl);
    queue.polled = true;
    const deadline = this.clock.now() + request.waitTimeSeconds * 1000;
    for (;;) {
      throwIfAborted(options?.signal);
      const received = this.take(queue, request);
      if (received.length > 0 || this.clock.now() >= deadline) {
        return received;
      }
      await this.waitForChange(queue, deadline, options?.signal);
    }
  }

  private deleteMessages(queueUrl: string, entries: SqsReceiptEntry[]): SqsBatchOutcome {
    this.authorize('DeleteMessageBatch');
    const queue = this.queueByUrl(queueUrl);
    validateBatchEntries(entries);
    const outcome: SqsBatchOutcome = { successful: [], failed: [] };
    for (const entry of entries) {
      const known = this.handles.get(entry.receiptHandle);
      if (known?.queue !== queue) {
        outcome.failed.push(failure(entry.id, 'ReceiptHandleIsInvalid', 'The specified receipt handle is not valid.'));
        continue;
      }
      // Only the most recent receipt handle deletes; an older one succeeds and does nothing.
      if (known.message.handle === entry.receiptHandle) {
        queue.messages = queue.messages.filter(message => message !== known.message);
      }
      outcome.successful.push(entry.id);
    }
    this.notify();
    return outcome;
  }

  private changeVisibility(queueUrl: string, entries: SqsVisibilityEntry[]): SqsBatchOutcome {
    this.authorize('ChangeMessageVisibilityBatch');
    const queue = this.queueByUrl(queueUrl);
    validateBatchEntries(entries);
    const outcome: SqsBatchOutcome = { successful: [], failed: [] };
    for (const entry of entries) {
      const problem = this.visibilityProblem(queue, entry);
      if (problem) {
        outcome.failed.push(failure(entry.id, problem.code, problem.message));
        continue;
      }
      const message = (this.handles.get(entry.receiptHandle) as { message: StoredMessage }).message;
      message.visibleAt = this.clock.now() + entry.visibilityTimeout * 1000;
      message.inFlight = false;
      outcome.successful.push(entry.id);
    }
    this.notify();
    return outcome;
  }

  private visibilityProblem(
    queue: QueueState,
    entry: SqsVisibilityEntry,
  ): { code: string; message: string } | undefined {
    const seconds = entry.visibilityTimeout;
    if (!Number.isInteger(seconds) || seconds < 0 || seconds > SQS_MAX_VISIBILITY_SECONDS) {
      return { code: 'InvalidParameterValue', message: `VisibilityTimeout must be 0 to ${SQS_MAX_VISIBILITY_SECONDS}.` };
    }
    const known = this.handles.get(entry.receiptHandle);
    if (known?.queue !== queue || !queue.messages.includes(known.message)) {
      return { code: 'ReceiptHandleIsInvalid', message: 'The specified receipt handle is not valid.' };
    }
    const now = this.clock.now();
    if (known.message.handle !== entry.receiptHandle || known.message.visibleAt <= now) {
      return { code: 'MessageNotInflight', message: "The specified message isn't in flight." };
    }
    const elapsed = (now - (known.message.receivedAt as number)) / 1000;
    if (elapsed + seconds > SQS_MAX_VISIBILITY_SECONDS) {
      return {
        code: 'InvalidParameterValue',
        message: 'The visibility timeout would pass 12 hours from the receive.',
      };
    }
    return undefined;
  }

  private sendMessages(queueUrl: string, entries: SqsSendEntry[]): SqsSendOutcome {
    this.authorize('SendMessageBatch');
    const queue = this.queueByUrl(queueUrl);
    validateBatchEntries(entries);
    const total = entries.reduce((sum, entry) => sum + sqsMessageBytes(entry.body, entry.messageAttributes), 0);
    if (total > SQS_MAX_MESSAGE_BYTES) {
      throw new InMemorySqsError('BatchRequestTooLong', `The batch is ${total} bytes, over ${SQS_MAX_MESSAGE_BYTES}.`);
    }
    return this.store(queue, entries);
  }

  private store(queue: QueueState, entries: SqsSendEntry[]): SqsSendOutcome {
    const outcome: SqsSendOutcome = { successful: [], failed: [] };
    const now = this.clock.now();
    for (const entry of entries) {
      const problem = sendProblem(queue, entry);
      if (problem) {
        outcome.failed.push({ id: entry.id, code: problem.code, message: problem.message, senderFault: true });
        continue;
      }
      const message: StoredMessage = {
        id: randomUUID(),
        body: entry.body,
        md5: createHash('md5').update(entry.body, 'utf8').digest('hex'),
        messageAttributes: { ...entry.messageAttributes },
        traceHeader: entry.traceHeader,
        sentAt: now,
        visibleAt: now + (entry.delaySeconds ?? queue.delaySeconds) * 1000,
        receiveCount: 0,
        inFlight: false,
      };
      queue.messages.push(message);
      outcome.successful.push({ id: entry.id, messageId: message.id });
    }
    this.notify();
    return outcome;
  }

  private getQueueUrl(queueName: string, ownerAccountId?: string): string {
    this.authorize('GetQueueUrl');
    if (ownerAccountId !== undefined && ownerAccountId !== this.accountId) {
      throw queueDoesNotExist(queueName);
    }
    if (!this.queues.has(queueName) && this.autoCreateQueues) {
      this.createQueue(queueName);
    }
    return this.queueByName(queueName).url;
  }

  private getQueueAttributes(queueUrl: string, names: string[]): Partial<Record<string, string>> {
    this.authorize('GetQueueAttributes');
    const queue = this.queueByUrl(queueUrl);
    const unknown = names.find(name => name !== 'All' && !QUEUE_ATTRIBUTES.has(name));
    if (unknown) {
      throw new InMemorySqsError('InvalidAttributeName', `Unknown attribute ${unknown}.`);
    }
    const all = this.attributesOf(queue);
    const wanted = names.includes('All') ? Object.keys(all) : names;
    const attributes: Partial<Record<string, string>> = {};
    for (const name of wanted) {
      if (all[name] !== undefined) {
        attributes[name] = all[name];
      }
    }
    return attributes;
  }

  private attributesOf(queue: QueueState): Partial<Record<string, string>> {
    const now = this.clock.now();
    const count = (predicate: (message: StoredMessage) => boolean): string =>
      String(queue.messages.filter(predicate).length);
    return {
      VisibilityTimeout: String(queue.visibilityTimeout),
      DelaySeconds: String(queue.delaySeconds),
      MaximumMessageSize: String(queue.maximumMessageSize),
      RedrivePolicy: queue.redrivePolicy,
      QueueArn: queue.arn,
      ApproximateNumberOfMessages: count(message => message.visibleAt <= now),
      ApproximateNumberOfMessagesNotVisible: count(message => message.visibleAt > now && message.receiveCount > 0),
      ApproximateNumberOfMessagesDelayed: count(message => message.visibleAt > now && message.receiveCount === 0),
    };
  }

  /** Receive up to `maxMessages` visible messages, redriving exhausted ones. */
  private take(queue: QueueState, request: SqsReceiveRequest): SqsReceivedMessage[] {
    const now = this.clock.now();
    const visibility = request.visibilityTimeout ?? queue.visibilityTimeout;
    const received: SqsReceivedMessage[] = [];
    for (const message of [...queue.messages]) {
      if (received.length >= request.maxMessages) {
        break;
      }
      if (message.visibleAt > now || this.redrive(queue, message)) {
        continue;
      }
      received.push(this.deliver(queue, message, visibility, now));
    }
    if (received.length > 0) {
      this.notify();
    }
    return received;
  }

  /** Move a message whose receives reached `maxReceiveCount` to the dead-letter queue. */
  private redrive(queue: QueueState, message: StoredMessage): boolean {
    const target = queue.redrive && this.queueByArn(queue.redrive.targetArn);
    if (!target || message.receiveCount < (queue.redrive as { maxReceiveCount: number }).maxReceiveCount) {
      return false;
    }
    queue.messages = queue.messages.filter(candidate => candidate !== message);
    target.messages.push({
      ...message,
      visibleAt: this.clock.now(),
      receiveCount: 0,
      firstReceiveAt: undefined,
      receivedAt: undefined,
      handle: undefined,
      inFlight: false,
      deadLetterSourceArn: queue.arn,
    });
    this.notify();
    return true;
  }

  private deliver(queue: QueueState, message: StoredMessage, visibility: number, now: number): SqsReceivedMessage {
    const handle = `${message.id}:${randomUUID()}`;
    message.receiveCount += 1;
    message.firstReceiveAt ??= now;
    message.receivedAt = now;
    message.handle = handle;
    message.visibleAt = now + visibility * 1000;
    message.inFlight = true;
    this.handles.set(handle, { queue, message });
    const attributes: Record<string, string> = {
      ApproximateReceiveCount: String(message.receiveCount),
      ApproximateFirstReceiveTimestamp: String(message.firstReceiveAt),
      SentTimestamp: String(message.sentAt),
      SenderId: this.accountId,
    };
    if (message.traceHeader !== undefined) {
      attributes.AWSTraceHeader = message.traceHeader;
    }
    if (message.deadLetterSourceArn !== undefined) {
      attributes.DeadLetterQueueSourceArn = message.deadLetterSourceArn;
    }
    return {
      MessageId: message.id,
      ReceiptHandle: handle,
      MD5OfBody: message.md5,
      Body: message.body,
      Attributes: attributes,
      ...(Object.keys(message.messageAttributes).length > 0
        ? { MessageAttributes: structuredClone(message.messageAttributes) }
        : {}),
    };
  }

  /** Wait until something changes, the next message becomes visible, or the wait ends. */
  private waitForChange(queue: QueueState, deadline: number, signal: AbortSignal | undefined): Promise<void> {
    const now = this.clock.now();
    const nextVisible = Math.min(
      deadline,
      ...queue.messages.map(message => message.visibleAt).filter(visibleAt => visibleAt > now),
    );
    return new Promise((resolve, reject) => {
      const finish = (): void => {
        this.clock.clearTimeout(timer);
        this.listeners.delete(finish);
        signal?.removeEventListener('abort', abort);
      };
      const wake = (): void => {
        finish();
        resolve();
      };
      const abort = (): void => {
        finish();
        reject(abortError());
      };
      const timer = this.clock.setTimeout(wake, nextVisible - now);
      this.listeners.add(wake);
      signal?.addEventListener('abort', abort, { once: true });
    });
  }

  private notify(): void {
    for (const listener of [...this.listeners]) {
      listener();
    }
  }

  private isIdle(): boolean {
    const now = this.clock.now();
    for (const queue of this.queues.values()) {
      if (queue.polled && queue.messages.some(message => message.inFlight || message.visibleAt <= now)) {
        return false;
      }
    }
    return true;
  }

  private describeBusy(): string {
    const now = this.clock.now();
    return [...this.queues.values()]
      .filter(queue => queue.polled)
      .map(queue => {
        const visible = queue.messages.filter(message => message.visibleAt <= now).length;
        const inFlight = queue.messages.filter(message => message.inFlight).length;
        return `${queue.name}: ${visible} visible, ${inFlight} in flight`;
      })
      .join('; ');
  }

  private authorize(operation: InMemorySqsOperation): void {
    if (this.denied.has(operation)) {
      throw new InMemorySqsError('AccessDenied', `Access to the resource is denied for sqs:${operation}.`);
    }
  }

  private arnOf(name: string): string {
    return `arn:aws:sqs:${this.region}:${this.accountId}:${name}`;
  }

  private queueByName(name: string): QueueState {
    const queue = this.queues.get(name);
    if (!queue) {
      throw queueDoesNotExist(name);
    }
    return queue;
  }

  private queueByUrl(url: string): QueueState {
    for (const queue of this.queues.values()) {
      if (queue.url === url) {
        return queue;
      }
    }
    throw queueDoesNotExist(url);
  }

  private queueByNameOrUrl(queue: string): QueueState {
    return /^https?:\/\//i.test(queue) ? this.queueByUrl(queue) : this.queueByName(queue);
  }

  private queueByArn(arn: string): QueueState | undefined {
    return [...this.queues.values()].find(queue => queue.arn === arn);
  }
}

function buildQueue(name: string, url: string, arn: string, attributes: InMemorySqsQueueAttributes): QueueState {
  const unknown = Object.keys(attributes).find(
    key => !['VisibilityTimeout', 'DelaySeconds', 'MaximumMessageSize', 'RedrivePolicy'].includes(key),
  );
  if (unknown) {
    throw new InMemorySqsError('InvalidAttributeName', `Unknown attribute ${unknown}.`);
  }
  return {
    name,
    url,
    arn,
    visibilityTimeout: integerAttribute(attributes, 'VisibilityTimeout', 30, 0, SQS_MAX_VISIBILITY_SECONDS),
    delaySeconds: integerAttribute(attributes, 'DelaySeconds', 0, 0, 900),
    maximumMessageSize: integerAttribute(attributes, 'MaximumMessageSize', SQS_MAX_MESSAGE_BYTES, 1_024, SQS_MAX_MESSAGE_BYTES),
    redrivePolicy: attributes.RedrivePolicy,
    redrive: attributes.RedrivePolicy === undefined ? undefined : parseRedrivePolicy(attributes.RedrivePolicy),
    messages: [],
    polled: false,
  };
}

function integerAttribute(
  attributes: InMemorySqsQueueAttributes,
  name: 'VisibilityTimeout' | 'DelaySeconds' | 'MaximumMessageSize',
  fallback: number,
  min: number,
  max: number,
): number {
  const raw = attributes[name];
  if (raw === undefined) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new InMemorySqsError('InvalidAttributeValue', `${name} must be an integer from ${min} to ${max}.`);
  }
  return value;
}

function parseRedrivePolicy(raw: string): { targetArn: string; maxReceiveCount: number } {
  let policy: { deadLetterTargetArn?: unknown; maxReceiveCount?: unknown };
  try {
    policy = JSON.parse(raw) as typeof policy;
  } catch {
    throw new InMemorySqsError('InvalidAttributeValue', 'RedrivePolicy is not valid JSON.');
  }
  const maxReceiveCount = Number(policy.maxReceiveCount);
  if (
    typeof policy.deadLetterTargetArn !== 'string' ||
    !Number.isInteger(maxReceiveCount) ||
    maxReceiveCount < 1 ||
    maxReceiveCount > 1_000
  ) {
    throw new InMemorySqsError(
      'InvalidAttributeValue',
      'RedrivePolicy needs a deadLetterTargetArn and a maxReceiveCount from 1 to 1000.',
    );
  }
  return { targetArn: policy.deadLetterTargetArn, maxReceiveCount };
}

function validateReceive(request: SqsReceiveRequest): void {
  const problems = [
    !Number.isInteger(request.maxMessages) || request.maxMessages < 1 || request.maxMessages > SQS_MAX_BATCH_ENTRIES
      ? `MaxNumberOfMessages must be 1 to ${SQS_MAX_BATCH_ENTRIES}`
      : undefined,
    !Number.isInteger(request.waitTimeSeconds) ||
    request.waitTimeSeconds < 0 ||
    request.waitTimeSeconds > SQS_MAX_WAIT_TIME_SECONDS
      ? `WaitTimeSeconds must be 0 to ${SQS_MAX_WAIT_TIME_SECONDS}`
      : undefined,
    request.visibilityTimeout !== undefined &&
    (!Number.isInteger(request.visibilityTimeout) ||
      request.visibilityTimeout < 0 ||
      request.visibilityTimeout > SQS_MAX_VISIBILITY_SECONDS)
      ? `VisibilityTimeout must be 0 to ${SQS_MAX_VISIBILITY_SECONDS}`
      : undefined,
    request.receiveRequestAttemptId !== undefined
      ? 'ReceiveRequestAttemptId applies only to FIFO queues'
      : undefined,
  ].filter(problem => problem !== undefined);
  if (problems.length > 0) {
    throw new InMemorySqsError('InvalidParameterValue', `${problems.join('; ')}.`);
  }
}

function validateBatchEntries(entries: { id: string }[]): void {
  if (entries.length === 0) {
    throw new InMemorySqsError('EmptyBatchRequest', 'The batch request does not contain any entries.');
  }
  if (entries.length > SQS_MAX_BATCH_ENTRIES) {
    throw new InMemorySqsError(
      'TooManyEntriesInBatchRequest',
      `The batch request contains more than ${SQS_MAX_BATCH_ENTRIES} entries.`,
    );
  }
  if (entries.some(entry => !/^[A-Za-z0-9_-]{1,80}$/.test(entry.id))) {
    throw new InMemorySqsError('InvalidBatchEntryId', 'A batch entry id does not abide by the specification.');
  }
  if (new Set(entries.map(entry => entry.id)).size !== entries.length) {
    throw new InMemorySqsError('BatchEntryIdsNotDistinct', 'Two or more batch entries have the same Id.');
  }
}

function sendProblem(queue: QueueState, entry: SqsSendEntry): { code: string; message: string } | undefined {
  if (entry.body.length === 0 || containsInvalidSqsCharacter(entry.body)) {
    return { code: 'InvalidMessageContents', message: 'The message contains characters outside the allowed set.' };
  }
  if (Object.keys(entry.messageAttributes ?? {}).length > SQS_MAX_MESSAGE_ATTRIBUTES) {
    return { code: 'InvalidParameterValue', message: `A message can carry at most ${SQS_MAX_MESSAGE_ATTRIBUTES} attributes.` };
  }
  if (sqsMessageBytes(entry.body, entry.messageAttributes) > queue.maximumMessageSize) {
    return { code: 'InvalidParameterValue', message: `Message must be shorter than ${queue.maximumMessageSize} bytes.` };
  }
  const delay = entry.delaySeconds;
  if (delay !== undefined && (!Number.isInteger(delay) || delay < 0 || delay > 900)) {
    return { code: 'InvalidParameterValue', message: 'DelaySeconds must be 0 to 900.' };
  }
  if (entry.messageDeduplicationId !== undefined) {
    return {
      code: 'InvalidParameterValue',
      message: 'MessageDeduplicationId is valid only for FIFO queues.',
    };
  }
  return undefined;
}

function failure(id: string, code: string, message: string): SqsBatchFailure {
  return { id, code, message, senderFault: true };
}

function queueDoesNotExist(queue: string): InMemorySqsError {
  return new InMemorySqsError('QueueDoesNotExist', `The specified queue does not exist: ${queue}.`);
}

function abortError(): Error {
  const error = new Error('The receive was aborted.');
  error.name = 'AbortError';
  return error;
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw abortError();
  }
}
