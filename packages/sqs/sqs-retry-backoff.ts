import { SQS_MAX_VISIBILITY_SECONDS } from './constants';

/**
 * How the delay is randomized: `'equal'` picks uniformly in `[d/2, d]`,
 * `'full'` in `[0, d]`, and `'none'` uses `d` exactly. A delay never drops
 * below one second, whatever the jitter.
 */
export type SqsRetryJitter = 'equal' | 'full' | 'none';

/**
 * How long a message whose failure maps to `'retry'` stays invisible. The delay
 * starts at `initialDelaySeconds` and grows by `multiplier` with every receive
 * of the message, up to `maxDelaySeconds`.
 *
 * The growth reads `ApproximateReceiveCount`, which SQS keeps per message: the
 * backoff survives a restart and is shared by every instance consuming the
 * queue, with no state in memory.
 */
export interface SqsRetryBackoffOptions {
  /** @default 1 */
  initialDelaySeconds?: number;
  /** @default 300 */
  maxDelaySeconds?: number;
  /** @default 2 */
  multiplier?: number;
  /** @default 'equal' */
  jitter?: SqsRetryJitter;
}

/** The defaults: the same backoff shape as `@nestjs/outbox`'s relay. */
export const DEFAULT_SQS_RETRY_BACKOFF: Readonly<Required<SqsRetryBackoffOptions>> = Object.freeze({
  initialDelaySeconds: 1,
  maxDelaySeconds: 300,
  multiplier: 2,
  jitter: 'equal',
});

const JITTERS: ReadonlySet<string> = new Set(['equal', 'full', 'none']);

/**
 * Validate and complete the `retryBackoff` option. `false` turns the backoff
 * off; a retried message then returns when its visibility timeout expires.
 *
 * @internal
 */
export function resolveRetryBackoff(
  options: SqsRetryBackoffOptions | false | undefined,
): Required<SqsRetryBackoffOptions> | undefined {
  if (options === false) {
    return undefined;
  }
  const resolved = { ...DEFAULT_SQS_RETRY_BACKOFF, ...options };
  const { initialDelaySeconds, maxDelaySeconds, multiplier, jitter } = resolved;
  const valid =
    Number.isInteger(initialDelaySeconds) &&
    initialDelaySeconds >= 1 &&
    Number.isInteger(maxDelaySeconds) &&
    maxDelaySeconds >= initialDelaySeconds &&
    maxDelaySeconds <= SQS_MAX_VISIBILITY_SECONDS &&
    multiplier >= 1 &&
    JITTERS.has(jitter);
  if (!valid) {
    throw new Error(
      'Invalid retryBackoff: expected integer seconds with 1 <= initialDelaySeconds <= maxDelaySeconds ' +
        `<= ${SQS_MAX_VISIBILITY_SECONDS}, multiplier >= 1 and jitter 'equal', 'full' or 'none'; got ` +
        `${JSON.stringify(resolved)}.`,
    );
  }
  return resolved;
}

/**
 * The visibility, in whole seconds, for the next attempt of a message received
 * `receiveCount` times. Always at least 1: a retry never makes a message
 * visible at once, so a failing message cannot spin.
 *
 * @internal
 */
export function retryDelaySeconds(
  backoff: Required<SqsRetryBackoffOptions>,
  receiveCount: number,
  random: () => number = Math.random,
): number {
  const exponent = Math.max(1, receiveCount) - 1;
  const base = Math.min(
    backoff.maxDelaySeconds,
    backoff.initialDelaySeconds * backoff.multiplier ** exponent,
  );
  return Math.max(1, Math.ceil(applyJitter(base, backoff.jitter, random)));
}

function applyJitter(base: number, jitter: SqsRetryJitter, random: () => number): number {
  if (jitter === 'none') {
    return base;
  }
  return jitter === 'full' ? random() * base : base / 2 + (random() * base) / 2;
}
