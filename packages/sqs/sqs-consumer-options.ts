import { SQS_MAX_BATCH_ENTRIES, SQS_MAX_VISIBILITY_SECONDS, SQS_MAX_WAIT_TIME_SECONDS } from './constants';
import { SqsConsumerOptions, SqsHeartbeatOptions } from './interfaces';
import { ResolvedSqsHeartbeat } from './sqs-heartbeat';

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
  /** The visibility heartbeat's timing; unset when the heartbeat is off. */
  heartbeat?: ResolvedSqsHeartbeat;
  /**
   * Why the heartbeat is off although it was not turned off (`heartbeat:
   * false`), for the one warning the consumer logs about it.
   */
  heartbeatOff?: string;
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
  const effectiveVisibilityTimeout = visibilityTimeout ?? queueVisibilityTimeout;
  const heartbeat = resolveHeartbeat(pick('heartbeat'), effectiveVisibilityTimeout);

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
    heartbeat.problem,
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
    effectiveVisibilityTimeout,
    heartbeat: heartbeat.timing,
    heartbeatOff: heartbeat.off,
  };
}

interface HeartbeatResolution {
  timing?: ResolvedSqsHeartbeat;
  off?: string;
  problem?: string;
}

/**
 * The heartbeat's timing against the consumer's visibility timeout `vt`:
 * extensions of `extendBySeconds` (default `vt`) every `intervalSeconds`
 * (default `max(1, floor(vt / 2))`). The interval must be shorter than both,
 * or a held message would become visible before its first extension.
 *
 * Off when turned off, and when `vt` is unknown. A `vt` too short for the
 * default timing (0 or 1 second) turns it off too; a configured timing that
 * cannot work is a configuration error.
 */
function resolveHeartbeat(
  option: SqsHeartbeatOptions | false | undefined,
  visibility: number | undefined,
): HeartbeatResolution {
  if (option === false) {
    return {};
  }
  const configured = option ?? {};
  const problem = heartbeatOptionProblem(configured);
  if (problem) {
    return { problem };
  }
  if (visibility === undefined) {
    return {
      off:
        "the visibility timeout is unknown (the queue's attributes could not be read and visibilityTimeout " +
        'is not set); set visibilityTimeout to turn it on',
    };
  }
  const extendBySeconds = configured.extendBySeconds ?? visibility;
  const intervalSeconds = configured.intervalSeconds ?? Math.max(1, Math.floor(visibility / 2));
  if (intervalSeconds < Math.min(visibility, extendBySeconds)) {
    return { timing: { intervalSeconds, extendBySeconds } };
  }
  if (configured.intervalSeconds === undefined && configured.extendBySeconds === undefined) {
    return { off: `the visibility timeout of ${visibility} s is too short to extend` };
  }
  return {
    problem:
      `heartbeat.intervalSeconds (${intervalSeconds}) must be shorter than the visibility timeout ` +
      `(${visibility} s) and than heartbeat.extendBySeconds (${extendBySeconds})`,
  };
}

function heartbeatOptionProblem(options: SqsHeartbeatOptions): string | undefined {
  const { intervalSeconds, extendBySeconds } = options;
  if (intervalSeconds !== undefined && !(Number.isInteger(intervalSeconds) && intervalSeconds >= 1)) {
    return 'heartbeat.intervalSeconds must be an integer >= 1';
  }
  if (
    extendBySeconds !== undefined &&
    !(Number.isInteger(extendBySeconds) && extendBySeconds >= 1 && extendBySeconds <= SQS_MAX_VISIBILITY_SECONDS)
  ) {
    return `heartbeat.extendBySeconds must be an integer from 1 to ${SQS_MAX_VISIBILITY_SECONDS}`;
  }
  return undefined;
}

function check(valid: boolean, problem: string): string | undefined {
  return valid ? undefined : problem;
}
