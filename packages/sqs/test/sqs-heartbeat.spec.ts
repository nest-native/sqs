import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SqsClock } from '../clock';
import { SqsBatchOutcome } from '../driver';
import { remainingVisibilitySeconds, SqsHeartbeat } from '../sqs-heartbeat';
import { SqsSettler } from '../sqs-settler';
import { VirtualClock } from '../testing/virtual-clock';
import { createFakeDriver, createRecordingLogger, deferred, FakeDriver, flush } from './helpers';

function harness(timing = { intervalSeconds: 10, extendBySeconds: 30 }, driver: FakeDriver = createFakeDriver()) {
  const clock = new VirtualClock(0);
  const logger = createRecordingLogger();
  const settler = new SqsSettler(driver, 'https://q/orders', 'orders', logger);
  const heartbeat = new SqsHeartbeat({ ...timing, settler, clock, logger, queueKey: 'orders' });
  const extensions = () =>
    driver.changes.map(call => call.entries.map(entry => `${entry.receiptHandle}:${entry.visibilityTimeout}`));
  return { clock, logger, driver, heartbeat, extensions };
}

describe('remainingVisibilitySeconds', () => {
  it('counts what is left of the 12 hours from the receive, in whole seconds, keeping 30 in reserve', () => {
    const clock = new VirtualClock(0);
    assert.equal(remainingVisibilitySeconds(clock, 0), 43_170);
    assert.equal(remainingVisibilitySeconds(clock, -1), 43_169, 'a started second counts as elapsed');
    assert.equal(remainingVisibilitySeconds(clock, -43_170_000), 0);
  });
});

describe('SqsHeartbeat', () => {
  it('extends a held message every interval from its receive, by extendBySeconds', async () => {
    const { clock, heartbeat, extensions } = harness();
    const beat = heartbeat.start({ messageId: 'a', receiptHandle: 'h-a', receivedAt: 0 });
    await clock.advance(9_999);
    assert.deepEqual(extensions(), [], 'a handler shorter than the interval costs no call');
    await clock.advance(1);
    assert.deepEqual(extensions(), [['h-a:30']]);
    await clock.advance(20_000);
    assert.deepEqual(extensions(), [['h-a:30'], ['h-a:30'], ['h-a:30']]);
    await beat.stop();
    await clock.advance(60_000);
    assert.equal(extensions().length, 3, 'nothing after stop()');
    assert.equal(clock.pendingTimers, 0);
  });

  it('extends the messages received together in one batch call', async () => {
    // Node runs every timer that is due in one timers phase, before any
    // setImmediate: the settler then sends the extensions as one batch.
    const timers = new Map<number, { at: number; callback: () => void }>();
    let now = 0;
    let sequence = 0;
    const timersPhaseClock: SqsClock = {
      now: () => now,
      setTimeout: (callback, ms) => {
        sequence += 1;
        timers.set(sequence, { at: now + ms, callback });
        return sequence;
      },
      clearTimeout: timer => timers.delete(timer as number),
    };
    const driver = createFakeDriver();
    const logger = createRecordingLogger();
    const settler = new SqsSettler(driver, 'https://q/orders', 'orders', logger);
    const heartbeat = new SqsHeartbeat({
      intervalSeconds: 10,
      extendBySeconds: 30,
      settler,
      clock: timersPhaseClock,
      logger,
      queueKey: 'orders',
    });
    const beats = ['a', 'b', 'c'].map(id => heartbeat.start({ messageId: id, receiptHandle: `h-${id}`, receivedAt: 0 }));
    now = 10_000;
    for (const [timer, { at, callback }] of [...timers]) {
      if (at <= now) {
        timers.delete(timer);
        callback();
      }
    }
    await flush();
    assert.deepEqual(
      driver.changes.map(call => call.entries.map(entry => `${entry.receiptHandle}:${entry.visibilityTimeout}`)),
      [['h-a:30', 'h-b:30', 'h-c:30']],
    );
    await Promise.all(beats.map(beat => beat.stop()));
    assert.equal(timers.size, 0);
  });

  it('clamps the extension that reaches the 12-hour budget, warns once, and stops', async () => {
    const { clock, logger, heartbeat, extensions } = harness({ intervalSeconds: 20_000, extendBySeconds: 21_000 });
    const beat = heartbeat.start({ messageId: 'long', receiptHandle: 'h-long', receivedAt: 0 });
    await clock.advance(40_000_000);
    // At 20 000 s, 23 170 s are left: a full extension. At 40 000 s, 3 170 s are left.
    assert.deepEqual(extensions(), [['h-long:21000'], ['h-long:3170']]);
    assert.equal(logger.warnings.length, 1);
    assert.match(logger.warnings[0], /Message long on "orders" has nearly used the 12 hours SQS allows/);
    await clock.advance(40_000_000);
    assert.equal(extensions().length, 2, 'no extension after the budget ran out');
    assert.equal(clock.pendingTimers, 0);
    await beat.stop();
  });

  it('sends nothing when nothing is left of the budget', async () => {
    const { clock, logger, heartbeat, extensions } = harness({ intervalSeconds: 1, extendBySeconds: 30 });
    heartbeat.start({ messageId: 'old', receiptHandle: 'h-old', receivedAt: -43_170_000 });
    await clock.advance(1_000);
    assert.deepEqual(extensions(), []);
    assert.equal(logger.warnings.length, 1);
    assert.equal(clock.pendingTimers, 0);
  });

  it('stops only once the extensions it sent are answered, so nothing overtakes them', async () => {
    const answered = deferred<SqsBatchOutcome>();
    const driver = createFakeDriver({ changeVisibility: () => answered.promise });
    const { clock, heartbeat } = harness(undefined, driver);
    const beat = heartbeat.start({ messageId: 'a', receiptHandle: 'h-a', receivedAt: 0 });
    await clock.advance(10_000);

    let stopped = false;
    const stopping = beat.stop().then(() => {
      stopped = true;
    });
    await flush();
    assert.equal(stopped, false, 'the extension in flight is still unanswered');
    answered.resolve({ successful: ['0'], failed: [] });
    await stopping;
    assert.equal(stopped, true);
    await beat.stop();
    assert.equal(clock.pendingTimers, 0);
  });

  it('keeps beating after an extension SQS refused', async () => {
    const driver = createFakeDriver({
      changeVisibility: async () => ({ successful: [], failed: [{ id: '0', code: 'InternalError', senderFault: false }] }),
    });
    const { clock, logger, heartbeat } = harness(undefined, driver);
    const beat = heartbeat.start({ messageId: 'a', receiptHandle: 'h-a', receivedAt: 0 });
    await clock.advance(20_000);
    assert.deepEqual(logger.warnings, [
      'SQS refused a visibility change on "orders": InternalError.',
      'SQS refused a visibility change on "orders": InternalError.',
    ]);
    await beat.stop();
  });
});
