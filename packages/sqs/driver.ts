/**
 * The driver seam over `@aws-sdk/client-sqs`.
 *
 * The package talks to SQS only through {@link SqsDriver}: six batch-shaped
 * operations, which is everything the consumer and the producer need. The
 * default {@link createAwsSqsDriver} drives the AWS SDK directly — there is no
 * `sqs-consumer` or `sqs-producer` layer underneath — and resolves the SDK
 * lazily, so importing the package never loads it. `InMemorySqs` (from
 * `@nest-native/sqs/testing`) implements the same seam for tests.
 *
 * The shapes below mirror the SDK's own, structurally, so this file never
 * imports the SDK's types: an SDK `Message` is assignable to {@link SqsReceivedMessage}.
 */

/** One message attribute, mirroring the SDK's `MessageAttributeValue`. */
export interface SqsMessageAttributeValue {
  DataType?: string;
  StringValue?: string;
  BinaryValue?: Uint8Array;
}

/**
 * A received message, mirroring the fields of the SDK's `Message` this package
 * reads. `Attributes` holds the system attributes (`ApproximateReceiveCount`,
 * `SentTimestamp`, `MessageGroupId`, …); `MessageAttributes` holds yours.
 */
export interface SqsReceivedMessage {
  MessageId?: string;
  ReceiptHandle?: string;
  MD5OfBody?: string;
  Body?: string;
  Attributes?: Partial<Record<string, string>>;
  MessageAttributes?: Record<string, SqsMessageAttributeValue>;
}

/** A `ReceiveMessage` call. The driver always asks for every attribute. */
export interface SqsReceiveRequest {
  queueUrl: string;
  /** `MaxNumberOfMessages`, 1 to 10. */
  maxMessages: number;
  /** `WaitTimeSeconds`, 0 to 20. */
  waitTimeSeconds: number;
  /** Per-receive `VisibilityTimeout`; the queue's own applies when omitted. */
  visibilityTimeout?: number;
  /** FIFO queues only: lets the SDK's own retries get the same messages back. */
  receiveRequestAttemptId?: string;
}

/** Per-call options. `signal` aborts the HTTP request. */
export interface SqsCallOptions {
  signal?: AbortSignal;
}

/** One entry of a delete batch. `id` is unique within the batch. */
export interface SqsReceiptEntry {
  id: string;
  receiptHandle: string;
}

/** One entry of a visibility batch. */
export interface SqsVisibilityEntry extends SqsReceiptEntry {
  visibilityTimeout: number;
}

/** One entry of a send batch, mirroring `SendMessageBatchRequestEntry`. */
export interface SqsSendEntry {
  id: string;
  body: string;
  delaySeconds?: number;
  messageAttributes?: Record<string, SqsMessageAttributeValue>;
  /** Sent as the `AWSTraceHeader` system attribute. */
  traceHeader?: string;
  messageGroupId?: string;
  messageDeduplicationId?: string;
}

/** An entry SQS refused, mirroring `BatchResultErrorEntry`. */
export interface SqsBatchFailure {
  id: string;
  code: string;
  message?: string;
  senderFault: boolean;
}

/**
 * The per-entry result of a delete or visibility batch. A batch call can
 * return HTTP 200 and still fail some entries, so every caller checks `failed`.
 */
export interface SqsBatchOutcome {
  successful: string[];
  failed: SqsBatchFailure[];
}

/** An entry SQS accepted, mirroring `SendMessageBatchResultEntry`. */
export interface SqsSendSuccess {
  id: string;
  messageId: string;
  sequenceNumber?: string;
}

/** The per-entry result of a send batch. */
export interface SqsSendOutcome {
  successful: SqsSendSuccess[];
  failed: SqsBatchFailure[];
}

/**
 * The surface the package uses to talk to SQS. Single-message operations are
 * batches of one: IAM authorizes `DeleteMessageBatch`,
 * `ChangeMessageVisibilityBatch` and `SendMessageBatch` with the same
 * permissions as their single forms, so nothing extra is needed.
 */
export interface SqsDriver {
  /** The underlying client, exposed through `@InjectSqsClient()`. */
  readonly client: unknown;
  receiveMessages(request: SqsReceiveRequest, options?: SqsCallOptions): Promise<SqsReceivedMessage[]>;
  deleteMessages(queueUrl: string, entries: SqsReceiptEntry[], options?: SqsCallOptions): Promise<SqsBatchOutcome>;
  changeVisibility(
    queueUrl: string,
    entries: SqsVisibilityEntry[],
    options?: SqsCallOptions,
  ): Promise<SqsBatchOutcome>;
  sendMessages(queueUrl: string, entries: SqsSendEntry[], options?: SqsCallOptions): Promise<SqsSendOutcome>;
  getQueueUrl(queueName: string, ownerAccountId?: string, options?: SqsCallOptions): Promise<string>;
  getQueueAttributes(
    queueUrl: string,
    attributeNames: string[],
    options?: SqsCallOptions,
  ): Promise<Partial<Record<string, string>>>;
  /** Release the client. Only a client the driver created is destroyed. */
  destroy(): void;
}

/**
 * Configuration forwarded untouched to `new SQSClient(config)`: `region`,
 * `endpoint`, `credentials`, `maxAttempts`, `requestHandler`, …
 */
export type SqsClientConfig = Record<string, unknown>;

/** Anything that sends SDK commands the way `SQSClient` does. */
export interface SqsClientLike {
  send(command: unknown, options?: { abortSignal?: AbortSignal }): Promise<unknown>;
  destroy?(): void;
}

/**
 * How a client is configured: an `SQSClient` configuration for the module to
 * create the client from, or an existing client to use as is.
 */
export type SqsClientInput = SqsClientConfig | SqsClientLike;

/**
 * Builds the driver for one configured client. Override it through
 * `SqsModuleOptions.driverFactory`, as `SqsTestModule` does with `InMemorySqs`.
 */
export type SqsDriverFactory = (client: SqsClientInput, name: string) => SqsDriver;

/** Whether `input` is an existing client rather than a configuration. */
export function isSqsClientLike(input: unknown): input is SqsClientLike {
  return (
    typeof input === 'object' &&
    input !== null &&
    typeof (input as { send?: unknown }).send === 'function'
  );
}

type CommandClass = new (input: Record<string, unknown>) => unknown;

/**
 * The slice of `@aws-sdk/client-sqs` the driver uses, captured structurally so
 * the package never imports the SDK's types.
 *
 * @internal
 */
export interface AwsSqsSdk {
  SQSClient: new (config: SqsClientConfig) => SqsClientLike;
  ReceiveMessageCommand: CommandClass;
  DeleteMessageBatchCommand: CommandClass;
  ChangeMessageVisibilityBatchCommand: CommandClass;
  SendMessageBatchCommand: CommandClass;
  GetQueueUrlCommand: CommandClass;
  GetQueueAttributesCommand: CommandClass;
}

interface SdkBatchOutput {
  Successful?: { Id?: string; MessageId?: string; SequenceNumber?: string }[];
  Failed?: { Id?: string; Code?: string; Message?: string; SenderFault?: boolean }[];
}

/**
 * The default driver factory: `@aws-sdk/client-sqs`, driven directly.
 *
 * A configuration creates an `SQSClient` the driver owns and destroys on
 * shutdown. An existing client is used as is and never destroyed. Commands are
 * built from the application's own `@aws-sdk/client-sqs`, so there is exactly
 * one SDK in the tree.
 */
export const createAwsSqsDriver: SqsDriverFactory = client => {
  const sdk = loadAwsSqsSdk();
  if (isSqsClientLike(client)) {
    return new AwsSqsDriver(sdk, client, false);
  }
  return new AwsSqsDriver(sdk, new sdk.SQSClient(client), true);
};

/**
 * Resolve `@aws-sdk/client-sqs`, with an error that says what to do when it
 * cannot be loaded. `load` exists for tests.
 *
 * @internal
 */
export function loadAwsSqsSdk(load: (id: string) => unknown = id => require(id)): AwsSqsSdk {
  try {
    return load('@aws-sdk/client-sqs') as AwsSqsSdk;
  } catch (cause) {
    const error = new Error(describeSdkLoadFailure(cause));
    (error as { cause?: unknown }).cause = cause;
    throw error;
  }
}

function describeSdkLoadFailure(cause: unknown): string {
  const code = (cause as { code?: unknown } | null)?.code;
  if (code === 'MODULE_NOT_FOUND') {
    return (
      'The peer "@aws-sdk/client-sqs" is not installed. Install it next to ' +
      '@nest-native/sqs, or supply a "driverFactory" through SqsModule.forRoot.'
    );
  }
  const detail = cause instanceof Error ? cause.message : String(cause);
  return `The peer "@aws-sdk/client-sqs" is installed but failed to load: ${detail}`;
}

/**
 * @internal
 */
export class AwsSqsDriver implements SqsDriver {
  constructor(
    private readonly sdk: AwsSqsSdk,
    readonly client: SqsClientLike,
    private readonly owned: boolean,
  ) {}

  async receiveMessages(request: SqsReceiveRequest, options?: SqsCallOptions): Promise<SqsReceivedMessage[]> {
    const input = compact({
      QueueUrl: request.queueUrl,
      MaxNumberOfMessages: request.maxMessages,
      WaitTimeSeconds: request.waitTimeSeconds,
      VisibilityTimeout: request.visibilityTimeout,
      ReceiveRequestAttemptId: request.receiveRequestAttemptId,
      MessageSystemAttributeNames: ['All'],
      MessageAttributeNames: ['All'],
    });
    const output = (await this.send(new this.sdk.ReceiveMessageCommand(input), options)) as {
      Messages?: SqsReceivedMessage[];
    };
    return output.Messages ?? [];
  }

  async deleteMessages(
    queueUrl: string,
    entries: SqsReceiptEntry[],
    options?: SqsCallOptions,
  ): Promise<SqsBatchOutcome> {
    const command = new this.sdk.DeleteMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: entries.map(entry => ({ Id: entry.id, ReceiptHandle: entry.receiptHandle })),
    });
    return toBatchOutcome((await this.send(command, options)) as SdkBatchOutput);
  }

  async changeVisibility(
    queueUrl: string,
    entries: SqsVisibilityEntry[],
    options?: SqsCallOptions,
  ): Promise<SqsBatchOutcome> {
    const command = new this.sdk.ChangeMessageVisibilityBatchCommand({
      QueueUrl: queueUrl,
      Entries: entries.map(entry => ({
        Id: entry.id,
        ReceiptHandle: entry.receiptHandle,
        VisibilityTimeout: entry.visibilityTimeout,
      })),
    });
    return toBatchOutcome((await this.send(command, options)) as SdkBatchOutput);
  }

  async sendMessages(queueUrl: string, entries: SqsSendEntry[], options?: SqsCallOptions): Promise<SqsSendOutcome> {
    const command = new this.sdk.SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: entries.map(toSdkSendEntry),
    });
    const output = (await this.send(command, options)) as SdkBatchOutput;
    return {
      successful: (output.Successful ?? []).map(entry =>
        compact({
          id: entry.Id ?? '',
          messageId: entry.MessageId ?? '',
          sequenceNumber: entry.SequenceNumber,
        }) as unknown as SqsSendSuccess,
      ),
      failed: toFailures(output.Failed),
    };
  }

  async getQueueUrl(queueName: string, ownerAccountId?: string, options?: SqsCallOptions): Promise<string> {
    const command = new this.sdk.GetQueueUrlCommand(
      compact({ QueueName: queueName, QueueOwnerAWSAccountId: ownerAccountId }),
    );
    const output = (await this.send(command, options)) as { QueueUrl?: string };
    if (!output.QueueUrl) {
      throw new Error(`GetQueueUrl returned no URL for the queue "${queueName}".`);
    }
    return output.QueueUrl;
  }

  async getQueueAttributes(
    queueUrl: string,
    attributeNames: string[],
    options?: SqsCallOptions,
  ): Promise<Partial<Record<string, string>>> {
    const command = new this.sdk.GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: attributeNames,
    });
    const output = (await this.send(command, options)) as { Attributes?: Record<string, string> };
    return output.Attributes ?? {};
  }

  destroy(): void {
    if (this.owned) {
      this.client.destroy?.();
    }
  }

  private send(command: unknown, options?: SqsCallOptions): Promise<unknown> {
    return options?.signal
      ? this.client.send(command, { abortSignal: options.signal })
      : this.client.send(command);
  }
}

function toSdkSendEntry(entry: SqsSendEntry): Record<string, unknown> {
  return compact({
    Id: entry.id,
    MessageBody: entry.body,
    DelaySeconds: entry.delaySeconds,
    MessageAttributes: entry.messageAttributes,
    MessageSystemAttributes:
      entry.traceHeader === undefined
        ? undefined
        : { AWSTraceHeader: { DataType: 'String', StringValue: entry.traceHeader } },
    MessageGroupId: entry.messageGroupId,
    MessageDeduplicationId: entry.messageDeduplicationId,
  });
}

function toBatchOutcome(output: SdkBatchOutput): SqsBatchOutcome {
  return {
    successful: (output.Successful ?? []).map(entry => entry.Id ?? ''),
    failed: toFailures(output.Failed),
  };
}

function toFailures(failed: SdkBatchOutput['Failed']): SqsBatchFailure[] {
  return (failed ?? []).map(entry =>
    compact({
      id: entry.Id ?? '',
      code: entry.Code ?? 'Unknown',
      message: entry.Message,
      senderFault: entry.SenderFault ?? false,
    }) as unknown as SqsBatchFailure,
  );
}

/** Drop `undefined` values, so requests carry only the fields that were set. */
function compact(input: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    if (value !== undefined) {
      output[key] = value;
    }
  }
  return output;
}
