import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { settlesWithin, sleep, systemClock } from '../clock';
import { SqsPermits } from '../sqs-permits';
import { VirtualClock } from '../testing/virtual-clock';

describe('systemClock', () => {
  it('reads wall-clock time and runs real timers', async () => {
    const before = Date.now();
    assert.ok(systemClock.now() >= before);
    let fired = false;
    await new Promise<void>(resolve => {
      systemClock.setTimeout(() => {
        fired = true;
        resolve();
      }, 1);
    });
    assert.equal(fired, true);
  });

  it('clears a timer before it fires', async () => {
    let fired = false;
    const timer = systemClock.setTimeout(() => {
      fired = true;
    }, 5);
    systemClock.clearTimeout(timer);
    await new Promise(resolve => setTimeout(resolve, 15));
    assert.equal(fired, false);
  });
});

describe('VirtualClock', () => {
  it('starts at 2026-01-01 unless told otherwise', () => {
    assert.equal(new VirtualClock().now(), Date.UTC(2026, 0, 1));
    assert.equal(new VirtualClock(42).now(), 42);
  });

  it('fires due timers in deadline order, then creation order, and only when time moves', async () => {
    const clock = new VirtualClock(0);
    const fired: string[] = [];
    clock.setTimeout(() => fired.push('b@200'), 200);
    clock.setTimeout(() => fired.push('a@100'), 100);
    clock.setTimeout(() => fired.push('c@200'), 200);
    clock.setTimeout(() => fired.push('d@-5'), -5);
    clock.setTimeout(() => fired.push('late'), 1_000);
    assert.equal(clock.pendingTimers, 5);
    assert.deepEqual(fired, []);

    await clock.advance(250);

    assert.deepEqual(fired, ['d@-5', 'a@100', 'b@200', 'c@200']);
    assert.equal(clock.now(), 250);
    assert.equal(clock.pendingTimers, 1);
  });

  it('fires a timer scheduled by another timer within the same advance', async () => {
    const clock = new VirtualClock(0);
    const fired: number[] = [];
    clock.setTimeout(() => {
      fired.push(clock.now());
      clock.setTimeout(() => fired.push(clock.now()), 50);
    }, 50);
    await clock.advance(100);
    assert.deepEqual(fired, [50, 100]);
  });

  it('forgets a cleared timer', async () => {
    const clock = new VirtualClock(0);
    let fired = false;
    const timer = clock.setTimeout(() => {
      fired = true;
    }, 10);
    clock.clearTimeout(timer);
    await clock.advance(20);
    assert.equal(fired, false);
  });
});

describe('sleep', () => {
  it('resolves once the time has passed on the clock', async () => {
    const clock = new VirtualClock(0);
    let done = false;
    const sleeping = sleep(clock, 1_000, new AbortController().signal).then(() => {
      done = true;
    });
    await clock.advance(999);
    assert.equal(done, false);
    await clock.advance(1);
    await sleeping;
    assert.equal(done, true);
    assert.equal(clock.pendingTimers, 0);
  });

  it('resolves early when the signal aborts, and at once when it already has', async () => {
    const clock = new VirtualClock(0);
    const controller = new AbortController();
    const sleeping = sleep(clock, 60_000, controller.signal);
    controller.abort();
    await sleeping;
    assert.equal(clock.pendingTimers, 0);

    await sleep(clock, 60_000, controller.signal);
    assert.equal(clock.pendingTimers, 0);
  });
});

describe('settlesWithin', () => {
  it('reports whether the work settled before the time ran out', async () => {
    const clock = new VirtualClock(0);
    assert.equal(await settlesWithin(clock, Promise.resolve('done'), 10), true);
    assert.equal(await settlesWithin(clock, Promise.reject(new Error('failed')), 10), true);

    const pending = settlesWithin(clock, new Promise(() => undefined), 10);
    await clock.advance(10);
    assert.equal(await pending, false);
  });
});

describe('SqsPermits', () => {
  it('counts permits taken and given back', () => {
    const permits = new SqsPermits(3);
    assert.equal(permits.total, 3);
    permits.take(2);
    assert.equal(permits.available, 1);
    permits.give(2);
    assert.equal(permits.available, 3);
  });

  it('answers at once when a permit is free, or the signal already aborted', async () => {
    const permits = new SqsPermits(1);
    assert.equal(await permits.waitForAvailable(new AbortController().signal), true);
    const aborted = new AbortController();
    aborted.abort();
    assert.equal(await permits.waitForAvailable(aborted.signal), false);
  });

  it('wakes every waiter when a permit is given back', async () => {
    const permits = new SqsPermits(1);
    permits.take(1);
    const signal = new AbortController().signal;
    const first = permits.waitForAvailable(signal);
    const second = permits.waitForAvailable(signal);
    permits.give(0);
    permits.take(0);
    permits.give(1);
    assert.deepEqual(await Promise.all([first, second]), [true, true]);
  });

  it('stops waiting when the signal aborts', async () => {
    const permits = new SqsPermits(1);
    permits.take(1);
    const controller = new AbortController();
    const waiting = permits.waitForAvailable(controller.signal);
    controller.abort();
    assert.equal(await waiting, false);
    permits.give(1);
    assert.equal(permits.available, 1);
  });
});
