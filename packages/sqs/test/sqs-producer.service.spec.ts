import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SqsDriver, SqsSendEntry } from '../driver';
import { SqsModuleOptions } from '../interfaces';
import { SqsClients } from '../sqs-clients';
import { SqsMessageValidationError } from '../sqs-message-validation';
import { SqsBatchSendError, SqsProducerService, SqsSendError } from '../sqs-producer.service';
import { SqsQueueRegistry } from '../sqs-queue-registry';
import { createFakeDriver } from './helpers';

function producer(driver: SqsDriver, options: SqsModuleOptions = {}): SqsProducerService {
  const clients = new SqsClients({ driverFactory: () => driver });
  const registry = new SqsQueueRegistry(
    { orders: 'https://q/123456789012/orders', ledger: 'https://q/123456789012/ledger.fifo', ...options.queues },
    clients,
  );
  return new SqsProducerService(options, registry);
}

describe('SqsProducerService.send', () => {
  it('serializes the body, encodes the attributes, and returns the message id', async () => {
    const driver = createFakeDriver();
    const result = await producer(driver).send('orders', {
      body: { id: 1 },
      attributes: { type: 'OrderPlaced', version: 2 },
      delaySeconds: 5,
      traceHeader: 'Root=1-abc',
    });
    assert.deepEqual(result, { messageId: 'id-0' });
    assert.deepEqual(driver.sends, [
      {
        queueUrl: 'https://q/123456789012/orders',
        entries: [
          {
            id: '0',
            body: '{"id":1}',
            delaySeconds: 5,
            messageAttributes: {
              type: { DataType: 'String', StringValue: 'OrderPlaced' },
              version: { DataType: 'Number', StringValue: '2' },
            },
            traceHeader: 'Root=1-abc',
            messageGroupId: undefined,
            messageDeduplicationId: undefined,
          },
        ],
      },
    ]);
  });

  it('sends to a FIFO queue with its group and returns the sequence number', async () => {
    const driver = createFakeDriver({
      sendMessages: async (_url, entries) => ({
        successful: [{ id: entries[0].id, messageId: 'm1', sequenceNumber: '18' }],
        failed: [],
      }),
    });
    const result = await producer(driver).send('ledger', { body: 'entry', groupId: 'acct-1', deduplicationId: 'e-1' });
    assert.deepEqual(result, { messageId: 'm1', sequenceNumber: '18' });
  });

  it('fails before sending a message SQS would refuse', async () => {
    const driver = createFakeDriver();
    await assert.rejects(
      producer(driver).send('ledger', { body: 'no group' }),
      (error: unknown) =>
        error instanceof SqsMessageValidationError &&
        error.message === 'The message for "ledger": A FIFO queue requires a groupId (MessageGroupId) on every message.',
    );
    await assert.rejects(producer(driver).send('orders', { body: new Uint8Array([1]) }), /The message for "orders": SQS message bodies are text/);
    const throwsString = {
      toJSON(): never {
        throw 'not serializable';
      },
    };
    await assert.rejects(producer(driver).send('orders', { body: throwsString }), /The message for "orders": not serializable/);
    await assert.rejects(
      producer(driver, { producer: { maxMessageBytes: 4 } }).send('orders', { body: 'too long' }),
      /over the 4-byte limit/,
    );
    assert.deepEqual(driver.sends, []);
  });

  it('rejects with what SQS said when it refuses the message', async () => {
    const refusing = createFakeDriver({
      sendMessages: async () => ({
        successful: [],
        failed: [{ id: '0', code: 'KmsAccessDenied', message: 'no key', senderFault: true }],
      }),
    });
    await assert.rejects(producer(refusing).send('orders', { body: 'x' }), (error: unknown) => {
      assert.ok(error instanceof SqsSendError);
      assert.equal(error.message, 'SQS refused the message for "orders": KmsAccessDenied (no key).');
      assert.equal(error.queue, 'orders');
      assert.equal(error.code, 'KmsAccessDenied');
      assert.equal(error.senderFault, true);
      return true;
    });

    const silent = createFakeDriver({ sendMessages: async () => ({ successful: [], failed: [] }) });
    await assert.rejects(producer(silent).send('orders', { body: 'x' }), {
      message: 'SQS refused the message for "orders": NoResult (SQS reported neither success nor failure).',
    });

    const bare = createFakeDriver({
      sendMessages: async () => ({ successful: [], failed: [{ id: '0', code: 'InternalError', senderFault: false }] }),
    });
    await assert.rejects(producer(bare).send('orders', { body: 'x' }), { message: 'SQS refused the message for "orders": InternalError.' });
  });

  it('passes the abort signal, and resolves logical queues', async () => {
    const driver = createFakeDriver();
    let seenSignal: AbortSignal | undefined;
    driver.sendMessages = async (_url, entries, options) => {
      seenSignal = options?.signal;
      return { successful: entries.map(entry => ({ id: entry.id, messageId: 'm' })), failed: [] };
    };
    const signal = new AbortController().signal;
    await producer(driver).send('orders', { body: 'x' }, { signal });
    assert.equal(seenSignal, signal);
    assert.deepEqual(await producer(driver).resolveQueue('ledger'), {
      key: 'ledger',
      name: 'ledger.fifo',
      url: 'https://q/123456789012/ledger.fifo',
      isFifo: true,
      client: 'default',
    });
  });

  it('refuses an invalid maxMessageBytes', () => {
    for (const maxMessageBytes of [0, 1_048_577, 1.5]) {
      assert.throws(() => producer(createFakeDriver(), { producer: { maxMessageBytes } }), /maxMessageBytes must be/);
    }
  });
});

describe('SqsProducerService.sendBatch', () => {
  it('sends nothing for an empty list', async () => {
    const driver = createFakeDriver();
    assert.deepEqual(await producer(driver).sendBatch('orders', []), []);
    assert.deepEqual(driver.sends, []);
  });

  it('splits into requests of at most 10 messages, and returns every result in order', async () => {
    const driver = createFakeDriver({
      sendMessages: async (_url, entries) => ({
        successful: [...entries].reverse().map(entry => ({ id: entry.id, messageId: `m${entry.id}` })),
        failed: [],
      }),
    });
    const results = await producer(driver).sendBatch(
      'orders',
      Array.from({ length: 23 }, (_, index) => ({ body: { index } })),
    );
    assert.equal(results.length, 23);
    assert.deepEqual(results[0], { messageId: 'm0' });
    assert.deepEqual(results[22], { messageId: 'm22' });
  });

  it('splits so no request carries more than 1 MiB', async () => {
    const sizes: number[][] = [];
    const driver = createFakeDriver({
      sendMessages: async (_url, entries: SqsSendEntry[]) => {
        sizes.push(entries.map(entry => entry.body.length));
        return { successful: entries.map(entry => ({ id: entry.id, messageId: entry.id })), failed: [] };
      },
    });
    await producer(driver).sendBatch(
      'orders',
      Array.from({ length: 6 }, () => ({ body: 'x'.repeat(300_000) })),
    );
    assert.deepEqual(sizes, [
      [300_000, 300_000, 300_000],
      [300_000, 300_000, 300_000],
    ]);
  });

  it('validates every message before sending any', async () => {
    const driver = createFakeDriver();
    await assert.rejects(
      producer(driver).sendBatch('orders', [{ body: 'ok' }, { body: '' }]),
      /Message 1 for "orders": The message body is empty/,
    );
    assert.deepEqual(driver.sends, []);
  });

  it('keeps going past refused messages on a standard queue, and reports each one', async () => {
    const driver = createFakeDriver({
      sendMessages: async (_url, entries) => ({
        successful: entries.filter(entry => entry.id !== '3').map(entry => ({ id: entry.id, messageId: `m${entry.id}` })),
        failed: entries
          .filter(entry => entry.id === '3')
          .map(entry => ({ id: entry.id, code: 'InvalidMessageContents', message: 'bad', senderFault: true })),
      }),
    });
    await assert.rejects(
      producer(driver).sendBatch('orders', Array.from({ length: 12 }, (_, index) => ({ body: `m${index}` }))),
      (error: unknown) => {
        assert.ok(error instanceof SqsBatchSendError);
        assert.equal(error.successful.length, 11);
        assert.deepEqual(error.failed, [{ index: 3, code: 'InvalidMessageContents', message: 'bad', senderFault: true }]);
        assert.deepEqual(error.uncertain, []);
        assert.deepEqual(error.notAttempted, []);
        assert.equal(error.message, '11 of 12 messages for "orders" were accepted (1 refused, 0 uncertain, 0 not attempted).');
        return true;
      },
    );
  });

  it('stops at the first refused message on a FIFO queue, so nothing overtakes it', async () => {
    const driver = createFakeDriver({
      sendMessages: async (_url, entries) => ({
        successful: entries.filter(entry => entry.id !== '2').map(entry => ({ id: entry.id, messageId: `m${entry.id}` })),
        failed: entries.filter(entry => entry.id === '2').map(entry => ({ id: entry.id, code: 'Throttled', senderFault: false })),
      }),
    });
    await assert.rejects(
      producer(driver).sendBatch(
        'ledger',
        Array.from({ length: 25 }, (_, index) => ({ body: `e${index}`, groupId: 'acct' })),
      ),
      (error: unknown) => {
        assert.ok(error instanceof SqsBatchSendError);
        assert.deepEqual(error.failed, [{ index: 2, code: 'Throttled', senderFault: false }]);
        assert.equal(error.successful.length, 9);
        assert.deepEqual(error.notAttempted, Array.from({ length: 15 }, (_, index) => index + 10));
        return true;
      },
    );
  });

  it('stops at a request that fails, reporting its messages as uncertain', async () => {
    let calls = 0;
    const cause = new Error('socket hang up');
    const driver = createFakeDriver({
      sendMessages: async (_url, entries) => {
        calls += 1;
        if (calls === 2) {
          throw cause;
        }
        return { successful: entries.map(entry => ({ id: entry.id, messageId: entry.id })), failed: [] };
      },
    });
    await assert.rejects(
      producer(driver).sendBatch('orders', Array.from({ length: 25 }, (_, index) => ({ body: `m${index}` }))),
      (error: unknown) => {
        assert.ok(error instanceof SqsBatchSendError);
        assert.equal(error.successful.length, 10);
        assert.deepEqual(error.uncertain, Array.from({ length: 10 }, (_, index) => index + 10));
        assert.deepEqual(error.notAttempted, [20, 21, 22, 23, 24]);
        assert.equal((error as { cause?: unknown }).cause, cause);
        return true;
      },
    );
    assert.equal(calls, 2);
  });
});
