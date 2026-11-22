import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { DEFAULT_SQS_RETRY_BACKOFF, resolveRetryBackoff, retryDelaySeconds } from '../sqs-retry-backoff';

describe('resolveRetryBackoff', () => {
  it('fills the defaults, keeps overrides, and turns off with false', () => {
    assert.deepEqual(resolveRetryBackoff(undefined), DEFAULT_SQS_RETRY_BACKOFF);
    assert.deepEqual(resolveRetryBackoff({ maxDelaySeconds: 60, jitter: 'none' }), {
      initialDelaySeconds: 1,
      maxDelaySeconds: 60,
      multiplier: 2,
      jitter: 'none',
    });
    assert.equal(resolveRetryBackoff(false), undefined);
  });

  it('refuses a backoff that could spin or exceed what SQS accepts', () => {
    for (const options of [
      { initialDelaySeconds: 0 },
      { initialDelaySeconds: 1.5 },
      { initialDelaySeconds: 10, maxDelaySeconds: 5 },
      { maxDelaySeconds: 2.5 },
      { maxDelaySeconds: 43_201 },
      { multiplier: 0.5 },
      { jitter: 'random' as never },
    ]) {
      assert.throws(() => resolveRetryBackoff(options), /Invalid retryBackoff/);
    }
  });
});

describe('retryDelaySeconds', () => {
  const none = { initialDelaySeconds: 2, maxDelaySeconds: 30, multiplier: 2, jitter: 'none' as const };

  it('grows with the receive count and stops at the cap', () => {
    assert.deepEqual(
      [0, 1, 2, 3, 4, 5, 50].map(count => retryDelaySeconds(none, count)),
      [2, 2, 4, 8, 16, 30, 30],
    );
  });

  it('randomizes within [d/2, d] with equal jitter and [0, d] with full jitter, never below 1 s', () => {
    const equal = { ...none, jitter: 'equal' as const };
    assert.equal(retryDelaySeconds(equal, 3, () => 0), 4);
    assert.equal(retryDelaySeconds(equal, 3, () => 0.999), 8);
    const full = { ...none, jitter: 'full' as const };
    assert.equal(retryDelaySeconds(full, 3, () => 0), 1);
    assert.equal(retryDelaySeconds(full, 3, () => 0.5), 4);
    const value = retryDelaySeconds(DEFAULT_SQS_RETRY_BACKOFF, 1);
    assert.ok(value >= 1 && value <= 1);
  });
});
