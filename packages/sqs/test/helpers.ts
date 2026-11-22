import { Logger, LoggerService } from '@nestjs/common';
import {
  SqsBatchOutcome,
  SqsDriver,
  SqsReceiptEntry,
  SqsReceivedMessage,
  SqsReceiveRequest,
  SqsSendEntry,
  SqsSendOutcome,
  SqsVisibilityEntry,
} from '../driver';

/** A logger that records what it is told, by level. */
export interface RecordingLogger extends LoggerService {
  logs: string[];
  warnings: string[];
  errors: string[];
  clear(): void;
}

export function createRecordingLogger(): RecordingLogger {
  const logger: RecordingLogger = {
    logs: [],
    warnings: [],
    errors: [],
    log: message => logger.logs.push(String(message)),
    warn: message => logger.warnings.push(String(message)),
    error: message => logger.errors.push(String(message)),
    debug: () => undefined,
    verbose: () => undefined,
    clear: () => {
      logger.logs.length = 0;
      logger.warnings.length = 0;
      logger.errors.length = 0;
    },
  };
  return logger;
}

/** Route Nest's static logger to a recording logger, and return it. */
export function captureNestLogs(): RecordingLogger {
  const logger = createRecordingLogger();
  Logger.overrideLogger(logger);
  return logger;
}

/** A driver whose every operation is a recorded stub, overridable per test. */
export interface FakeDriver extends SqsDriver {
  receives: SqsReceiveRequest[];
  deletes: { queueUrl: string; entries: SqsReceiptEntry[] }[];
  changes: { queueUrl: string; entries: SqsVisibilityEntry[] }[];
  sends: { queueUrl: string; entries: SqsSendEntry[] }[];
  destroyed: number;
}

export function createFakeDriver(overrides: Partial<SqsDriver> = {}): FakeDriver {
  const driver: FakeDriver = {
    client: { fake: true },
    receives: [],
    deletes: [],
    changes: [],
    sends: [],
    destroyed: 0,
    receiveMessages: async (request: SqsReceiveRequest): Promise<SqsReceivedMessage[]> => {
      driver.receives.push(request);
      return [];
    },
    deleteMessages: async (queueUrl, entries): Promise<SqsBatchOutcome> => {
      driver.deletes.push({ queueUrl, entries });
      return { successful: entries.map(entry => entry.id), failed: [] };
    },
    changeVisibility: async (queueUrl, entries): Promise<SqsBatchOutcome> => {
      driver.changes.push({ queueUrl, entries });
      return { successful: entries.map(entry => entry.id), failed: [] };
    },
    sendMessages: async (queueUrl, entries): Promise<SqsSendOutcome> => {
      driver.sends.push({ queueUrl, entries });
      return {
        successful: entries.map(entry => ({ id: entry.id, messageId: `id-${entry.id}` })),
        failed: [],
      };
    },
    getQueueUrl: async name => `https://sqs.us-east-1.amazonaws.com/123456789012/${name}`,
    getQueueAttributes: async () => ({ VisibilityTimeout: '30', RedrivePolicy: '{}' }),
    destroy: () => {
      driver.destroyed += 1;
    },
  };
  return Object.assign(driver, overrides);
}

/** A received message with a receipt handle. */
export function receivedMessage(id: string, body = `{"id":"${id}"}`, receiveCount = 1): SqsReceivedMessage {
  return {
    MessageId: id,
    ReceiptHandle: `handle-${id}`,
    Body: body,
    Attributes: { ApproximateReceiveCount: String(receiveCount) },
  };
}

/** A promise and its resolvers. */
export function deferred<T>(): { promise: Promise<T>; resolve(value: T): void; reject(error: unknown): void } {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Let promise chains and `setImmediate` work run. */
export async function flush(rounds = 10): Promise<void> {
  for (let round = 0; round < rounds; round += 1) {
    await new Promise<void>(resolve => setImmediate(resolve));
  }
}
