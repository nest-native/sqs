import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SqsContext } from '../sqs-context';
import { SqsDispatcher, SqsDispatcherOptions, SqsHandlerInvocation } from '../sqs-dispatcher';
import { SqsErrorMapper } from '../sqs-error-mapping';
import { SqsPermits } from '../sqs-permits';
import { resolveRetryBackoff } from '../sqs-retry-backoff';
import { SqsSettler } from '../sqs-settler';
import { VirtualClock } from '../testing/virtual-clock';
import { createFakeDriver, createRecordingLogger, deferred, FakeDriver, flush, receivedMessage } from './helpers';

const QUEUE = { key: 'orders', name: 'orders', url: 'https://q/orders', isFifo: false, client: 'default' };

function harness(
  run: (invocation: SqsHandlerInvocation) => Promise<unknown>,
  overrides: Partial<SqsDispatcherOptions> & { driver?: FakeDriver } = {},
) {
  const clock = new VirtualClock(0);
  const logger = createRecordingLogger();
  const driver = overrides.driver ?? createFakeDriver();
  const settler = new SqsSettler(driver, QUEUE.url, QUEUE.key, logger);
  const permits = new SqsPermits(5);
  const dispatcher = new SqsDispatcher({
    queue: QUEUE,
    run,
    errorMapper: () => 'retry',
    retryBackoff: resolveRetryBackoff({ jitter: 'none' }),
    settler,
    permits,
    clock,
    logger,
    random: () => 0,
    ...overrides,
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
