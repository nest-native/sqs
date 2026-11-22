import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SqsBatchOutcome, SqsReceivedMessage } from '../driver';
import { SqsContext } from '../sqs-context';
import { SqsDispatcher, SqsDispatcherOptions, SqsHandlerInvocation } from '../sqs-dispatcher';
import { SqsErrorMapper } from '../sqs-error-mapping';
import { SqsHeartbeat } from '../sqs-heartbeat';
import { SqsPermits } from '../sqs-permits';
import { resolveRetryBackoff } from '../sqs-retry-backoff';
import { SqsSettler } from '../sqs-settler';
import { VirtualClock } from '../testing/virtual-clock';
import { createFakeDriver, createRecordingLogger, deferred, FakeDriver, flush, receivedMessage } from './helpers';

const QUEUE = { key: 'orders', name: 'orders', url: 'https://q/orders', isFifo: false, client: 'default' };
const FIFO_QUEUE = { key: 'ledger', name: 'ledger.fifo', url: 'https://q/ledger.fifo', isFifo: true, client: 'default' };

interface HarnessOverrides extends Partial<SqsDispatcherOptions> {
  driver?: FakeDriver;
  /** Turn the visibility heartbeat on, on the harness's clock and settler. */
  heartbeatTiming?: { intervalSeconds: number; extendBySeconds: number };
}

function harness(run: (invocation: SqsHandlerInvocation) => Promise<unknown>, overrides: HarnessOverrides = {}) {
  const { driver: givenDriver, heartbeatTiming, ...options } = overrides;
  const queue = options.queue ?? QUEUE;
  const clock = new VirtualClock(0);
  const logger = createRecordingLogger();
  const driver = givenDriver ?? createFakeDriver();
  const settler = new SqsSettler(driver, queue.url, queue.key, logger);
  const permits = new SqsPermits(5);
  const heartbeat =
    heartbeatTiming && new SqsHeartbeat({ ...heartbeatTiming, settler, clock, logger, queueKey: queue.key });
  const dispatcher = new SqsDispatcher({
    queue,
    run,
    errorMapper: () => 'retry',
    retryBackoff: resolveRetryBackoff({ jitter: 'none' }),
    settler,
    permits,
    clock,
    logger,
    random: () => 0,
    heartbeat,
    ...options,
  });
  return { clock, logger, driver, settler, permits, dispatcher };
}

function deliver(dispatcher: SqsDispatcher, permits: SqsPermits, id: string, receiveCount = 1): void {
  permits.take(1);
  dispatcher.dispatch(receivedMessage(id, `{"id":"${id}"}`, receiveCount));
}

describe('SqsDispatcher', () => {
  it('deletes a message once its handler returns, and gives its permit back', async () => {
    const seen: unknown[] = [];
    const { driver, permits, dispatcher } = harness(async ({ payload, context }) => {
      seen.push(payload, context.getMessageId());
    });
    deliver(dispatcher, permits, 'a');
    assert.equal(dispatcher.size, 1);
    await flush();
    assert.deepEqual(seen, [{ id: 'a' }, 'a']);
    assert.deepEqual(driver.deletes[0].entries, [{ id: '0', receiptHandle: 'handle-a' }]);
    assert.equal(dispatcher.size, 0);
    assert.equal(permits.available, 5);
  });

  it('retries a failure after the backoff for its receive count, never deleting it', async () => {
    const { driver, logger, permits, dispatcher } = harness(async () => {
      throw new Error('downstream timeout');
    });
    deliver(dispatcher, permits, 'a', 1);
    deliver(dispatcher, permits, 'b', 4);
    await flush();
    assert.deepEqual(driver.deletes, []);
    assert.deepEqual(driver.changes[0].entries, [
      { id: '0', receiptHandle: 'handle-a', visibilityTimeout: 1 },
      { id: '1', receiptHandle: 'handle-b', visibilityTimeout: 8 },
    ]);
    assert.match(logger.warnings[0], /Message a on "orders" failed: downstream timeout; retrying it\./);
  });

  it('deletes a failure the error mapper maps to delete', async () => {
    const mapped: string[] = [];
    const errorMapper: SqsErrorMapper = async (error, context) => {
      mapped.push(`${(error as Error).message}@${context.getMessageId()}`);
      return 'delete' as const;
    };
    const { driver, logger, permits, dispatcher } = harness(
      async () => {
        throw new Error('poison');
      },
      { errorMapper },
    );
    deliver(dispatcher, permits, 'a');
    await flush();
    assert.deepEqual(mapped, ['poison@a']);
    assert.equal(driver.deletes.length, 1);
    assert.match(logger.warnings[0], /failed: poison; deleting it\./);
  });

  it('retries when the error mapper throws or answers anything else', async () => {
    const throwing = harness(
      async () => {
        throw 'string failure';
      },
      {
        errorMapper: () => {
          throw new Error('mapper bug');
        },
      },
    );
    deliver(throwing.dispatcher, throwing.permits, 'a');
    await flush();
    assert.match(throwing.logger.errors[0], /The SQS error mapper threw .* \(mapper bug\); retrying the message\./);
    assert.match(throwing.logger.warnings[0], /failed: string failure; retrying it\./);
    assert.equal(throwing.driver.changes.length, 1);

    const odd = harness(
      async () => {
        throw new Error('x');
      },
      { errorMapper: (() => 'commit') as unknown as SqsErrorMapper },
    );
    deliver(odd.dispatcher, odd.permits, 'a');
    await flush();
    assert.equal(odd.driver.changes.length, 1);
    assert.equal(odd.driver.deletes.length, 0);

    const silent = harness(
      async () => {
        throw new Error('x');
      },
      {
        errorMapper: () => {
          throw 'not an error';
        },
      },
    );
    deliver(silent.dispatcher, silent.permits, 'a');
    await flush();
    assert.match(silent.logger.errors[0], /\(not an error\)/);
  });

  it('leaves the visibility untouched when the backoff is off', async () => {
    const { driver, permits, dispatcher } = harness(
      async () => {
        throw new Error('x');
      },
      { retryBackoff: undefined },
    );
    deliver(dispatcher, permits, 'a');
    await flush();
    assert.deepEqual(driver.changes, []);
    assert.deepEqual(driver.deletes, []);
  });

  it('keeps a retry within the 12 hours SQS allows from the receive', async () => {
    const gate = deferred<void>();
    const { clock, driver, permits, dispatcher } = harness(async () => {
      await gate.promise;
      throw new Error('x');
    });
    deliver(dispatcher, permits, 'a', 20);
    await clock.advance((43_200 - 30 - 100) * 1000);
    gate.resolve();
    await flush();
    assert.equal(driver.changes[0].entries[0].visibilityTimeout, 100, 'clamped to the budget left');

    const late = deferred<void>();
    const exhausted = harness(async () => {
      await late.promise;
      throw new Error('x');
    });
    deliver(exhausted.dispatcher, exhausted.permits, 'b');
    await exhausted.clock.advance((43_200 - 30) * 1000);
    late.resolve();
    await flush();
    assert.deepEqual(exhausted.driver.changes, [], 'no budget left: the message returns when it expires');
  });

  it('lets a handler delete its message itself, once', async () => {
    let context!: SqsContext;
    const { driver, permits, dispatcher } = harness(async invocation => {
      context = invocation.context;
      await context.delete();
      await context.delete();
      throw new Error('after the delete');
    });
    deliver(dispatcher, permits, 'a');
    await flush();
    assert.equal(driver.deletes.length, 1);
    assert.deepEqual(driver.changes, [], 'an error after ctx.delete() cannot retry');
    await context.delete();
    assert.equal(driver.deletes.length, 1);
  });

  it('rejects ctx.delete() and ctx.changeVisibility() when SQS refuses them', async () => {
    const driver = createFakeDriver({
      deleteMessages: async () => ({ successful: [], failed: [{ id: '0', code: 'ReceiptHandleIsInvalid', senderFault: true }] }),
      changeVisibility: async () => ({ successful: [], failed: [{ id: '0', code: 'MessageNotInflight', senderFault: true }] }),
    });
    const errors: string[] = [];
    const { permits, dispatcher } = harness(
      async ({ context }) => {
        await context.changeVisibility(60).catch((error: Error) => errors.push(error.message));
        await context.changeVisibility(-1).catch((error: Error) => errors.push(error.message));
        await context.changeVisibility(1.5).catch((error: Error) => errors.push(error.message));
        await context.changeVisibility(43_201).catch((error: Error) => errors.push(error.message));
        await context.delete().catch((error: Error) => errors.push(error.message));
      },
      { driver },
    );
    deliver(dispatcher, permits, 'a');
    await flush();
    assert.deepEqual(errors, [
      'SQS did not change the visibility of message a.',
      'A visibility timeout is an integer from 0 to 43200 seconds.',
      'A visibility timeout is an integer from 0 to 43200 seconds.',
      'A visibility timeout is an integer from 0 to 43200 seconds.',
      'SQS did not delete message a.',
    ]);
  });

  it('extends the visibility on request, then still deletes on return', async () => {
    const { driver, permits, dispatcher } = harness(async ({ context }) => {
      await context.changeVisibility(600);
    });
    deliver(dispatcher, permits, 'a');
    await flush();
    assert.equal(driver.changes[0].entries[0].visibilityTimeout, 600);
    assert.equal(driver.deletes.length, 1);
  });

  it('drains at once when nothing is in flight, and waits for what is', async () => {
    const gate = deferred<void>();
    const { clock, driver, permits, dispatcher } = harness(() => gate.promise);
    await dispatcher.drain(1_000);

    deliver(dispatcher, permits, 'a');
    const draining = dispatcher.drain(1_000);
    await clock.advance(500);
    gate.resolve();
    await draining;
    assert.equal(driver.deletes.length, 1);
    assert.deepEqual(driver.changes, []);
  });

  it('releases what outlives the drain, aborts its signal, and ignores its outcome', async () => {
    const gate = deferred<void>();
    let signal!: AbortSignal;
    const { clock, driver, logger, permits, dispatcher, settler } = harness(async ({ context }) => {
      signal = context.signal;
      await gate.promise;
    });
    deliver(dispatcher, permits, 'slow');
    await flush();
    const draining = dispatcher.drain(2_000);
    await clock.advance(2_000);
    await draining;
    await settler.flush();

    assert.equal(signal.aborted, true);
    assert.deepEqual(driver.changes[0].entries, [{ id: '0', receiptHandle: 'handle-slow', visibilityTimeout: 0 }]);
    assert.match(logger.warnings[0], /1 message\(s\) on "orders" were still being handled after 2000 ms/);

    gate.resolve();
    await flush();
    assert.deepEqual(driver.deletes, [], 'a released message is not deleted when its handler finishes late');
    assert.equal(permits.available, 5);
  });

  it('does not release a message whose settle is already under way', async () => {
    const settling = deferred<void>();
    const driver = createFakeDriver({
      deleteMessages: async (_url, entries) => {
        await settling.promise;
        return { successful: entries.map(entry => entry.id), failed: [] };
      },
    });
    const { clock, permits, dispatcher } = harness(async () => undefined, { driver });
    deliver(dispatcher, permits, 'a');
    await flush();
    const draining = dispatcher.drain(1_000);
    await clock.advance(1_000);
    await draining;
    assert.deepEqual(driver.changes, []);
    settling.resolve();
    await flush();
  });
});

/** A FIFO message of `group` (its id names it: `a1`, `a2`, …). */
function fifoMessage(id: string, group: string | undefined, receiveCount = 1): SqsReceivedMessage {
  return {
    MessageId: id,
    ReceiptHandle: `handle-${id}`,
    Body: `{"id":"${id}"}`,
    Attributes: {
      ApproximateReceiveCount: String(receiveCount),
      ...(group === undefined ? {} : { MessageGroupId: group }),
    },
  };
}

/**
 * A FIFO dispatcher whose handler the test drives: each message's handler
 * waits for `finish(id)` or `fail(id)`.
 */
function fifoHarness(overrides: HarnessOverrides = {}) {
  const started: string[] = [];
  const gates = new Map<string, ReturnType<typeof deferred<void>>>();
  const gate = (id: string) => {
    let entry = gates.get(id);
    if (!entry) {
      entry = deferred<void>();
      gates.set(id, entry);
    }
    return entry;
  };
  const setup = harness(
    async ({ context }) => {
      started.push(context.getMessageId());
      await gate(context.getMessageId()).promise;
    },
    { queue: FIFO_QUEUE, ...overrides },
  );
  const deliverFifo = (id: string, group: string | undefined, receiveCount = 1) => {
    setup.permits.take(1);
    setup.dispatcher.dispatch(fifoMessage(id, group, receiveCount));
  };
  const finish = async (id: string) => {
    gate(id).resolve();
    await flush();
  };
  const fail = async (id: string) => {
    gate(id).reject(new Error(`${id} failed`));
    await flush();
  };
  const changes = () =>
    setup.driver.changes.map(call => call.entries.map(entry => `${entry.receiptHandle}:${entry.visibilityTimeout}`));
  const deleted = () => setup.driver.deletes.flatMap(call => call.entries.map(entry => entry.receiptHandle));
  return { ...setup, started, deliverFifo, finish, fail, changes, deleted };
}

describe('SqsDispatcher on a FIFO queue', () => {
  it('runs one message of a group at a time, in receive order, and different groups at once', async () => {
    const { started, deliverFifo, finish, deleted, permits, dispatcher } = fifoHarness();
    deliverFifo('a1', 'a');
    deliverFifo('a2', 'a');
    deliverFifo('b1', 'b');
    deliverFifo('a3', 'a');
    await flush();
    assert.deepEqual(started, ['a1', 'b1']);
    assert.equal(dispatcher.size, 4, 'the waiting messages are held too');
    assert.equal(permits.available, 1, 'each held message keeps its permit, waiting or running');

    await finish('b1');
    assert.deepEqual(deleted(), ['handle-b1']);
    assert.deepEqual(started, ['a1', 'b1'], 'another group settling does not unblock this one');

    await finish('a1');
    assert.deepEqual(started, ['a1', 'b1', 'a2']);
    await finish('a2');
    await finish('a3');
    assert.deepEqual(started, ['a1', 'b1', 'a2', 'a3']);
    assert.deepEqual(deleted(), ['handle-b1', 'handle-a1', 'handle-a2', 'handle-a3']);
    assert.equal(permits.available, 5);
    assert.equal(dispatcher.size, 0);
  });

  it('starts the next message of a group only once its predecessor is deleted', async () => {
    const deleting = deferred<SqsBatchOutcome>();
    const driver = createFakeDriver({ deleteMessages: () => deleting.promise });
    const { started, deliverFifo, finish } = fifoHarness({ driver });
    deliverFifo('a1', 'a');
    deliverFifo('a2', 'a');
    await flush();
    await finish('a1');
    assert.deepEqual(started, ['a1'], 'the delete of a1 is not answered yet');
    deleting.resolve({ successful: ['0'], failed: [] });
    await flush();
    assert.deepEqual(started, ['a1', 'a2']);
  });

  it('releases the messages held behind one whose delete SQS refused, instead of running them', async () => {
    const driver = createFakeDriver({
      deleteMessages: async (_queueUrl, entries) => ({
        successful: [],
        failed: entries.map(entry => ({ id: entry.id, code: 'InternalError', senderFault: false })),
      }),
    });
    const { started, deliverFifo, finish, changes, permits, dispatcher } = fifoHarness({ driver });
    deliverFifo('a1', 'a');
    deliverFifo('a2', 'a');
    deliverFifo('a3', 'a');
    await flush();
    await finish('a1');
    assert.deepEqual(started, ['a1'], 'a1 is still in SQS, so nothing of its group may overtake it');
    assert.deepEqual(changes(), [['handle-a2:0', 'handle-a3:0']]);
    assert.equal(dispatcher.size, 0);
    assert.equal(permits.available, 5);
  });

  it('releases the messages held behind one whose handler could not delete it itself', async () => {
    const driver = createFakeDriver({
      deleteMessages: async (_queueUrl, entries) => ({
        successful: [],
        failed: entries.map(entry => ({ id: entry.id, code: 'InternalError', senderFault: false })),
      }),
    });
    const errors: string[] = [];
    const started: string[] = [];
    const { permits, dispatcher } = harness(
      async ({ context }) => {
        started.push(context.getMessageId());
        await context.delete().catch((error: Error) => errors.push(error.message));
      },
      { queue: FIFO_QUEUE, driver },
    );
    for (const id of ['a1', 'a2']) {
      permits.take(1);
      dispatcher.dispatch(fifoMessage(id, 'a'));
    }
    await flush();
    assert.deepEqual(started, ['a1']);
    assert.deepEqual(errors, ['SQS did not delete message a1.']);
    assert.deepEqual(
      driver.changes.map(call => call.entries.map(entry => `${entry.receiptHandle}:${entry.visibilityTimeout}`)),
      [['handle-a2:0']],
    );
    assert.equal(dispatcher.size, 0);
  });

  it('on a retry, sets the backoff first, then releases the messages held behind it, giving their permits back', async () => {
    const backoffSet = deferred<SqsBatchOutcome>();
    const driver = createFakeDriver();
    const recordChange = driver.changeVisibility;
    driver.changeVisibility = async (queueUrl, entries) => {
      const outcome = await recordChange(queueUrl, entries);
      if (entries[0].receiptHandle === 'handle-a1') {
        return backoffSet.promise;
      }
      return outcome;
    };
    const { started, deliverFifo, fail, finish, changes, deleted, permits, logger } = fifoHarness({ driver });
    deliverFifo('a1', 'a', 3);
    deliverFifo('a2', 'a');
    deliverFifo('a3', 'a');
    deliverFifo('b1', 'b');
    await flush();
    await fail('a1');
    assert.deepEqual(changes(), [['handle-a1:4']], 'nothing is released before the backoff is set');
    assert.equal(permits.available, 1);

    backoffSet.resolve({ successful: ['0'], failed: [] });
    await flush();
    assert.deepEqual(changes(), [['handle-a1:4'], ['handle-a2:0', 'handle-a3:0']]);
    assert.equal(permits.available, 4, 'a1, a2 and a3 gave their permits back; b1 still holds one');
    assert.deepEqual(started, ['a1', 'b1'], 'a2 and a3 never ran');
    assert.match(logger.warnings[0], /Message a1 on "ledger" failed: a1 failed; retrying it\./);

    await finish('b1');
    assert.deepEqual(deleted(), ['handle-b1'], 'the other group was not held');
    assert.equal(permits.available, 5);
  });

  it('releases more than ten held messages ten per call', async () => {
    const { deliverFifo, fail, changes } = fifoHarness();
    deliverFifo('a0', 'a');
    for (let index = 1; index <= 12; index += 1) {
      deliverFifo(`a${index}`, 'a');
    }
    await flush();
    await fail('a0');
    assert.deepEqual(
      changes().map(call => call.length),
      [1, 10, 2],
    );
    assert.equal(changes()[1][0], 'handle-a1:0');
  });

  it('still releases the messages held behind a failure when the backoff is off, or the mapper throws', async () => {
    const off = fifoHarness({ retryBackoff: undefined });
    off.deliverFifo('a1', 'a');
    off.deliverFifo('a2', 'a');
    await flush();
    await off.fail('a1');
    assert.deepEqual(off.changes(), [['handle-a2:0']], 'the failed message keeps its visibility');
    assert.equal(off.permits.available, 5);

    const throwing = fifoHarness({
      errorMapper: () => {
        throw new Error('mapper bug');
      },
    });
    throwing.deliverFifo('a1', 'a');
    throwing.deliverFifo('a2', 'a');
    await flush();
    await throwing.fail('a1');
    assert.deepEqual(throwing.changes(), [['handle-a1:1'], ['handle-a2:0']]);
    assert.deepEqual(throwing.started, ['a1']);
  });

  it('releases nothing for a retry with nothing held behind it', async () => {
    const { deliverFifo, fail, changes, permits } = fifoHarness();
    deliverFifo('a1', 'a');
    await flush();
    await fail('a1');
    assert.deepEqual(changes(), [['handle-a1:1']]);
    assert.equal(permits.available, 5);
  });

  it('moves on when the mapper deletes the failure, or the handler deleted its message itself', async () => {
    const deleting = fifoHarness({ errorMapper: () => 'delete' });
    deleting.deliverFifo('a1', 'a');
    deleting.deliverFifo('a2', 'a');
    await flush();
    await deleting.fail('a1');
    assert.deepEqual(deleting.started, ['a1', 'a2']);
    assert.deepEqual(deleting.changes(), []);

    const started: string[] = [];
    const manual = harness(
      async ({ context }) => {
        started.push(context.getMessageId());
        if (context.getMessageId() === 'a1') {
          await context.delete();
          throw new Error('after the delete');
        }
      },
      { queue: FIFO_QUEUE },
    );
    for (const id of ['a1', 'a2']) {
      manual.permits.take(1);
      manual.dispatcher.dispatch(fifoMessage(id, 'a'));
    }
    await flush();
    assert.deepEqual(started, ['a1', 'a2']);
    assert.deepEqual(manual.driver.changes, [], 'nothing was released: a1 cannot be retried');
    assert.equal(manual.driver.deletes.flatMap(call => call.entries).length, 2);
  });

  it('runs FIFO messages that carry no group one at a time', async () => {
    const { started, deliverFifo, finish } = fifoHarness();
    deliverFifo('x1', undefined);
    deliverFifo('x2', undefined);
    await flush();
    assert.deepEqual(started, ['x1']);
    await finish('x1');
    assert.deepEqual(started, ['x1', 'x2']);
  });

  it('releases the messages waiting behind a running one, and lets the running ones finish', async () => {
    const { started, deliverFifo, finish, changes, deleted, permits, dispatcher, logger, clock } = fifoHarness();
    deliverFifo('a1', 'a');
    deliverFifo('a2', 'a');
    deliverFifo('a3', 'a');
    deliverFifo('b1', 'b');
    await flush();
    await dispatcher.releaseWaiting();
    assert.deepEqual(changes(), [['handle-a2:0', 'handle-a3:0']]);
    assert.equal(permits.available, 3, 'the released messages gave their permits back');
    assert.match(logger.logs[0], /Releasing 2 message\(s\) on "ledger" that were waiting behind an earlier message/);

    const draining = dispatcher.drain(10_000);
    await clock.advance(100);
    await finish('a1');
    await finish('b1');
    await draining;
    assert.deepEqual(started, ['a1', 'b1'], 'nothing new started once the waiting messages were released');
    assert.deepEqual(deleted(), ['handle-a1', 'handle-b1']);
    assert.equal(permits.available, 5);
    assert.equal(dispatcher.size, 0);
  });

  it('has nothing to release when no message is waiting', async () => {
    const standard = harness(async () => undefined);
    await standard.dispatcher.releaseWaiting();
    deliver(standard.dispatcher, standard.permits, 'a');
    await standard.dispatcher.releaseWaiting();
    await flush();
    assert.deepEqual(standard.driver.changes, []);
    assert.deepEqual(standard.logger.logs, []);
  });
});

describe('SqsDispatcher with the visibility heartbeat', () => {
  const heartbeatTiming = { intervalSeconds: 10, extendBySeconds: 30 };

  it('keeps every held message invisible, the waiting ones included, until it is settled', async () => {
    const { clock, deliverFifo, finish, changes, deleted } = fifoHarness({ heartbeatTiming });
    deliverFifo('a1', 'a');
    deliverFifo('a2', 'a');
    await flush();
    await clock.advance(10_000);
    assert.deepEqual(changes().flat(), ['handle-a1:30', 'handle-a2:30'], 'the waiting a2 is extended with the running a1');

    await finish('a1');
    await clock.advance(10_000);
    assert.deepEqual(changes().flat(), ['handle-a1:30', 'handle-a2:30', 'handle-a2:30'], 'a1 settled: its heartbeat stopped');
    await finish('a2');
    assert.deepEqual(deleted(), ['handle-a1', 'handle-a2']);
    await clock.advance(60_000);
    assert.equal(changes().flat().length, 3);
    assert.equal(clock.pendingTimers, 0);
  });

  it('settles a message only once the extension in flight for it is answered', async () => {
    const extending = deferred<SqsBatchOutcome>();
    const driver = createFakeDriver();
    const recordChange = driver.changeVisibility;
    driver.changeVisibility = async (queueUrl, entries) => {
      const outcome = await recordChange(queueUrl, entries);
      return entries[0].visibilityTimeout === 30 ? extending.promise : outcome;
    };
    const { clock, deliverFifo, finish, fail, changes, deleted } = fifoHarness({ driver, heartbeatTiming });
    deliverFifo('a1', 'a');
    deliverFifo('b1', 'b');
    await flush();
    await clock.advance(10_000);
    await finish('a1');
    await fail('b1');
    assert.deepEqual(deleted(), [], 'the delete waits for the extension');
    assert.deepEqual(changes().flat(), ['handle-a1:30', 'handle-b1:30'], 'and so does the retry');

    extending.resolve({ successful: ['0'], failed: [] });
    await flush();
    assert.deepEqual(deleted(), ['handle-a1']);
    assert.deepEqual(changes().flat(), ['handle-a1:30', 'handle-b1:30', 'handle-b1:1']);
  });

  it('stops for a message whose handler takes over its visibility or deletes it', async () => {
    const done = deferred<void>();
    const { clock, driver, permits, dispatcher } = harness(
      async ({ context }) => {
        if (context.getMessageId() === 'a1') {
          await context.changeVisibility(120);
        } else {
          await context.delete();
        }
        await done.promise;
      },
      { queue: FIFO_QUEUE, heartbeatTiming },
    );
    for (const [id, group] of [
      ['a1', 'a'],
      ['b1', 'b'],
    ]) {
      permits.take(1);
      dispatcher.dispatch(fifoMessage(id, group));
    }
    await flush();
    await clock.advance(30_000);
    assert.deepEqual(
      driver.changes.flatMap(call => call.entries.map(entry => `${entry.receiptHandle}:${entry.visibilityTimeout}`)),
      ['handle-a1:120'],
      'no extension for a message whose visibility the handler set, or that it deleted',
    );
    assert.equal(driver.deletes.length, 1);
    done.resolve();
    await flush();
    assert.equal(driver.deletes.flatMap(call => call.entries).length, 2);
    assert.equal(clock.pendingTimers, 0);
  });

  it('extends a message that came without a MessageId like any other', async () => {
    const { clock, permits, dispatcher, finish, changes } = fifoHarness({ heartbeatTiming });
    permits.take(1);
    dispatcher.dispatch({ ReceiptHandle: 'handle-anonymous', Body: '{}', Attributes: { MessageGroupId: 'a' } });
    await flush();
    await clock.advance(10_000);
    assert.deepEqual(changes().flat(), ['handle-anonymous:30']);
    await finish('');
    assert.equal(clock.pendingTimers, 0);
  });

  it('stops the heartbeat of what shutdown releases', async () => {
    const { clock, deliverFifo, changes, dispatcher } = fifoHarness({ heartbeatTiming });
    deliverFifo('a1', 'a');
    deliverFifo('a2', 'a');
    await flush();
    await dispatcher.releaseWaiting();
    const draining = dispatcher.drain(5_000);
    await clock.advance(5_000);
    await draining;
    assert.deepEqual(changes().flat(), ['handle-a2:0', 'handle-a1:0']);
    await clock.advance(60_000);
    assert.equal(changes().flat().length, 2, 'released messages are no longer extended');
    assert.equal(clock.pendingTimers, 0);
  });
});
