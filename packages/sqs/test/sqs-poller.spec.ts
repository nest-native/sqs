import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SqsReceivedMessage, SqsReceiveRequest } from '../driver';
import { SqsPermits } from '../sqs-permits';
import { SqsPoller } from '../sqs-poller';
import { ResolvedSqsQueue } from '../sqs-queue-registry';
import { VirtualClock } from '../testing/virtual-clock';
import { createFakeDriver, createRecordingLogger, deferred, flush, receivedMessage } from './helpers';

interface PendingReceive {
  request: SqsReceiveRequest;
  signal?: AbortSignal;
  resolve(messages: SqsReceivedMessage[]): void;
  reject(error: unknown): void;
}

/** A poller over a driver whose receives the test answers one by one. */
function harness(options: { concurrency?: number; maxMessages?: number; isFifo?: boolean } = {}) {
  const clock = new VirtualClock(0);
  const logger = createRecordingLogger();
  const receives: PendingReceive[] = [];
  const driver = createFakeDriver({
    receiveMessages: (request, callOptions) => {
      const pending = deferred<SqsReceivedMessage[]>();
      const receive: PendingReceive = {
        request,
        signal: callOptions?.signal,
        resolve: pending.resolve,
        reject: pending.reject,
      };
      callOptions?.signal?.addEventListener('abort', () => {
        const error = new Error('Request aborted');
        error.name = 'AbortError';
        pending.reject(error);
      });
      receives.push(receive);
      return pending.promise;
    },
  });
  const queue: ResolvedSqsQueue = {
    key: 'orders',
    name: options.isFifo ? 'orders.fifo' : 'orders',
    url: 'https://q/orders',
    isFifo: options.isFifo ?? false,
    client: 'default',
    driver,
  };
  const permits = new SqsPermits(options.concurrency ?? 3);
  const dispatched: SqsReceivedMessage[] = [];
  const released: SqsReceivedMessage[][] = [];
  const poller = new SqsPoller({
    queue,
    maxMessages: options.maxMessages ?? 10,
    waitTimeSeconds: 20,
    receiveTimeoutMs: 30_000,
    visibilityTimeout: 45,
    permits,
    clock,
    logger,
    dispatch: message => dispatched.push(message),
    release: messages => released.push(messages),
  });
  return { clock, logger, receives, permits, dispatched, released, poller };
}

describe('SqsPoller', () => {
  it('asks for no more messages than it has free permits, and dispatches what arrives', async () => {
    const { receives, permits, dispatched, poller } = harness({ concurrency: 3 });
    poller.start();
    poller.start();
    await flush();

    assert.equal(receives.length, 1);
    assert.deepEqual(receives[0].request, {
      queueUrl: 'https://q/orders',
      maxMessages: 3,
      waitTimeSeconds: 20,
      visibilityTimeout: 45,
      receiveRequestAttemptId: undefined,
    });

    receives[0].resolve([receivedMessage('a'), receivedMessage('b')]);
    await flush();
    assert.deepEqual(
      dispatched.map(message => message.MessageId),
      ['a', 'b'],
    );
    assert.equal(receives[1].request.maxMessages, 1, 'the next receive asks only for the free permit');
    assert.equal(permits.available, 0, 'two permits held by messages, one by the receive in flight');

    receives[1].resolve([receivedMessage('c')]);
    await flush();
    assert.equal(permits.available, 0);
    assert.equal(receives.length, 2, 'no receive while every permit is held');

    permits.give(2);
    await flush();
    assert.equal(receives[2].request.maxMessages, 2);
    receives[2].resolve([]);
    await flush();
    await poller.stop(true);
  });

  it('caps a receive at maxMessages', async () => {
    const { receives, poller } = harness({ concurrency: 50, maxMessages: 4 });
    poller.start();
    await flush();
    assert.equal(receives[0].request.maxMessages, 4);
    await poller.stop(true);
  });

  it('sends a ReceiveRequestAttemptId on a FIFO queue only', async () => {
    const fifo = harness({ isFifo: true });
    fifo.poller.start();
    await flush();
    assert.match(String(fifo.receives[0].request.receiveRequestAttemptId), /^[0-9a-f-]{36}$/);
    await fifo.poller.stop(true);
  });

  it('skips messages without a receipt handle, with a warning', async () => {
    const { receives, permits, dispatched, logger, poller } = harness({ concurrency: 2 });
    poller.start();
    await flush();
    receives[0].resolve([{ MessageId: 'orphan', Body: 'x' }, receivedMessage('a')]);
    await flush();
    assert.deepEqual(
      dispatched.map(message => message.MessageId),
      ['a'],
    );
    assert.equal(receives[1].request.maxMessages, 1, "the orphan's permit went straight to the next receive");
    assert.equal(permits.available, 0);
    assert.match(logger.warnings[0], /1 message\(s\) without a receipt handle on "orders"/);
    await poller.stop(true);
  });

  it('aborts a receive that outlives its deadline and backs off before the next', async () => {
    const { clock, receives, logger, poller } = harness();
    poller.start();
    await flush();
    await clock.advance(30_000);
    assert.equal(receives[0].signal?.aborted, true);
    assert.match(logger.warnings[0], /Receiving from "orders" failed \(no response within 30000 ms\); retrying in 1000 ms \(failure 1 in a row\)/);
    assert.equal(receives.length, 1);
    await clock.advance(1_000);
    assert.equal(receives.length, 2);
    await poller.stop(true);
  });

  it('doubles the backoff per failure up to 30 s, and resets it after a success', async () => {
    const { clock, receives, logger, poller } = harness();
    poller.start();
    const delays: number[] = [];
    for (let failure = 0; failure < 7; failure += 1) {
      await flush();
      receives[receives.length - 1].reject(failure % 2 === 0 ? new Error('Inaccessible host') : 'socket hang up');
      await flush();
      delays.push(Number(/retrying in (\d+) ms/.exec(logger.warnings[logger.warnings.length - 1])?.[1]));
      await clock.advance(30_000);
    }
    assert.deepEqual(delays, [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]);

    receives[receives.length - 1].resolve([]);
    await flush();
    receives[receives.length - 1].reject(new Error('again'));
    await flush();
    assert.match(logger.warnings[logger.warnings.length - 1], /retrying in 1000 ms \(failure 1 in a row\)/);
    await poller.stop(false);
  });

  it('stops without aborting the receive in flight, and releases what it returns', async () => {
    const { receives, permits, dispatched, released, poller } = harness({ concurrency: 2 });
    poller.start();
    await flush();
    const stopped = poller.stop(false);
    await flush();
    assert.equal(receives[0].signal?.aborted, false, 'the long poll is left to return');

    receives[0].resolve([receivedMessage('late')]);
    await stopped;

    assert.deepEqual(dispatched, []);
    assert.deepEqual(
      released.map(batch => batch.map(message => message.MessageId)),
      [['late']],
    );
    assert.equal(permits.available, 2);
  });

  it('stops quietly when the last receive returns nothing or fails', async () => {
    const empty = harness();
    empty.poller.start();
    await flush();
    const stopping = empty.poller.stop(false);
    empty.receives[0].resolve([]);
    await stopping;
    assert.deepEqual(empty.released, []);

    const failing = harness();
    failing.poller.start();
    await flush();
    const failingStop = failing.poller.stop(false);
    failing.receives[0].reject(new Error('connection reset'));
    await failingStop;
    assert.deepEqual(failing.logger.warnings, []);
  });

  it('aborts the receive in flight when asked to', async () => {
    const { receives, logger, poller } = harness();
    poller.start();
    await flush();
    await poller.stop(true);
    assert.equal(receives[0].signal?.aborted, true);
    assert.deepEqual(logger.warnings, []);
  });

  it('stops while waiting for a permit, and in the middle of a backoff', async () => {
    const waiting = harness({ concurrency: 1 });
    waiting.permits.take(1);
    waiting.poller.start();
    await flush();
    assert.equal(waiting.receives.length, 0);
    await waiting.poller.stop(false);

    const backingOff = harness();
    backingOff.poller.start();
    await flush();
    backingOff.receives[0].reject(new Error('down'));
    await flush();
    assert.equal(backingOff.clock.pendingTimers, 1, 'the backoff timer is pending');
    await backingOff.poller.stop(false);
    assert.equal(backingOff.clock.pendingTimers, 0);
    assert.equal(backingOff.receives.length, 1);
  });

  it('stops a poller that never started', async () => {
    const { poller } = harness();
    await poller.stop(false);
  });
});
