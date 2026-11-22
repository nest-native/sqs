import { SQS_MAX_BATCH_ENTRIES, SQS_MAX_VISIBILITY_SECONDS, SQS_MAX_WAIT_TIME_SECONDS } from './constants';
import { SqsConsumerOptions } from './interfaces';

/**
 * The settings of one consumer, resolved.
 *
 * @internal
 */
export interface ResolvedConsumerOptions {
  concurrency: number;
  maxMessages: number;
  waitTimeSeconds: number;
  receiveTimeoutMs: number;
  /** The `visibilityTimeout` option, passed on every receive when set. */
  visibilityTimeout?: number;
  /**
   * The visibility a received message actually gets: the option when set,
   * the queue's `VisibilityTimeout` attribute otherwise, unknown when neither
   * could be read.
   */
  effectiveVisibilityTimeout?: number;
}

const DEFAULT_CONCURRENCY = 1;
const RECEIVE_TIMEOUT_GRACE_MS = 10_000;

/**
 * Merge the consumer settings of one handler — the queue's configuration, then
 * the decorator's options, then `consumerDefaults`, the first that sets a value
 * winning — and validate them against what SQS accepts.
 *
 * @internal
 */
export function resolveConsumerOptions(
  layers: SqsConsumerOptions[],
  queueVisibilityTimeout: number | undefined,
  owner: string,
): ResolvedConsumerOptions {
  const pick = <K extends keyof SqsConsumerOptions>(key: K): SqsConsumerOptions[K] =>
    layers.find(layer => layer[key] !== undefined)?.[key];
  const concurrency = pick('concurrency') ?? DEFAULT_CONCURRENCY;
  const maxMessages = pick('maxMessages') ?? SQS_MAX_BATCH_ENTRIES;
  const waitTimeSeconds = pick('waitTimeSeconds') ?? SQS_MAX_WAIT_TIME_SECONDS;
  const receiveTimeoutMs = pick('receiveTimeoutMs') ?? waitTimeSeconds * 1000 + RECEIVE_TIMEOUT_GRACE_MS;
  const visibilityTimeout = pick('visibilityTimeout');

  const problems = [
    check(Number.isInteger(concurrency) && concurrency >= 1, 'concurrency must be an integer >= 1'),
    check(
      Number.isInteger(maxMessages) && maxMessages >= 1 && maxMessages <= SQS_MAX_BATCH_ENTRIES,
      `maxMessages must be an integer from 1 to ${SQS_MAX_BATCH_ENTRIES}`,
    ),
    check(
      Number.isInteger(waitTimeSeconds) && waitTimeSeconds >= 1 && waitTimeSeconds <= SQS_MAX_WAIT_TIME_SECONDS,
      `waitTimeSeconds must be an integer from 1 to ${SQS_MAX_WAIT_TIME_SECONDS} (short polling is not supported)`,
    ),
    check(
      receiveTimeoutMs > waitTimeSeconds * 1000,
      'receiveTimeoutMs must exceed the long poll (waitTimeSeconds * 1000)',
    ),
    check(
      visibilityTimeout === undefined ||
        (Number.isInteger(visibilityTimeout) && visibilityTimeout >= 0 && visibilityTimeout <= SQS_MAX_VISIBILITY_SECONDS),
      `visibilityTimeout must be an integer from 0 to ${SQS_MAX_VISIBILITY_SECONDS}`,
    ),
  ].filter((problem): problem is string => problem !== undefined);
  if (problems.length > 0) {
    throw new Error(`Invalid consumer options for ${owner}: ${problems.join('; ')}.`);
  }

  return {
    concurrency,
    maxMessages,
    waitTimeSeconds,
    receiveTimeoutMs,
    visibilityTimeout,
    effectiveVisibilityTimeout: visibilityTimeout ?? queueVisibilityTimeout,
  };
}

function check(valid: boolean, problem: string): string | undefined {
  return valid ? undefined : problem;
}
