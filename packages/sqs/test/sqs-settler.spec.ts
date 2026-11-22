import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SqsSettler } from '../sqs-settler';
import { createFakeDriver, createRecordingLogger, flush } from './helpers';

describe('SqsSettler', () => {
  it('coalesces deletes and visibility changes made in the same macrotask', async () => {
    const driver = createFakeDriver();
    const settler = new SqsSettler(driver, 'https://q', 'orders', createRecordingLogger());

    const results = await Promise.all([
      settler.delete('h1'),
      settler.delete('h2'),
      settler.changeVisibility('h3', 0),
      settler.changeVisibility('h4', 30),
    ]);

    assert.deepEqual(results, [true, true, true, true]);
    assert.deepEqual(driver.deletes, [
      {
        queueUrl: 'https://q',
        entries: [
          { id: '0', receiptHandle: 'h1' },
          { id: '1', receiptHandle: 'h2' },
        ],
      },
    ]);
    assert.deepEqual(driver.changes, [
      {
        queueUrl: 'https://q',
        entries: [
          { id: '0', receiptHandle: 'h3', visibilityTimeout: 0 },
          { id: '1', receiptHandle: 'h4', visibilityTimeout: 30 },
        ],
      },
    ]);
  });

  it('sends a full batch of 10 at once and splits beyond it', async () => {
    const driver = createFakeDriver();
    const settler = new SqsSettler(driver, 'https://q', 'orders', createRecordingLogger());
    const pending = Array.from({ length: 12 }, (_, index) => settler.delete(`h${index}`));
    assert.equal(driver.deletes.length, 1, 'the tenth delete sends the batch without waiting');
    await Promise.all(pending);
    assert.deepEqual(
      driver.deletes.map(call => call.entries.length),
      [10, 2],
    );
  });

  it('reports refused entries as unsettled, and logs why', async () => {
    const logger = createRecordingLogger();
    const driver = createFakeDriver({
      deleteMessages: async () => ({
        successful: ['0'],
        failed: [
          { id: '1', code: 'ReceiptHandleIsInvalid', message: 'stale', senderFault: true },
          { id: '2', code: 'InternalError', senderFault: false },
        ],
      }),
    });
    const settler = new SqsSettler(driver, 'https://q', 'orders', logger);
    const results = await Promise.all([settler.delete('a'), settler.delete('b'), settler.delete('c')]);
    assert.deepEqual(results, [true, false, false]);
    assert.deepEqual(logger.warnings, [
      'SQS refused a delete on "orders": ReceiptHandleIsInvalid (stale).',
      'SQS refused a delete on "orders": InternalError.',
    ]);
  });

  it('reports every entry unsettled when the call fails', async () => {
    const logger = createRecordingLogger();
    const driver = createFakeDriver({
      changeVisibility: async () => {
        throw new Error('socket hang up');
      },
      deleteMessages: async () => {
        throw 'throttled';
      },
    });
    const settler = new SqsSettler(driver, 'https://q', 'orders', logger);
    const results = await Promise.all([settler.changeVisibility('a', 0), settler.changeVisibility('b', 0), settler.delete('c')]);
    assert.deepEqual(results, [false, false, false]);
    assert.deepEqual(logger.warnings, [
      'A delete of 1 message(s) on "orders" failed: throttled.',
      'A visibility change of 2 message(s) on "orders" failed: socket hang up.',
    ]);
  });

  it('flushes what is pending without waiting for the next macrotask', async () => {
    const driver = createFakeDriver();
    const settler = new SqsSettler(driver, 'https://q', 'orders', createRecordingLogger());
    const pending = settler.delete('h1');
    await settler.flush();
    assert.equal(driver.deletes.length, 1);
    assert.equal(await pending, true);
    await flush();
    assert.equal(driver.deletes.length, 1, 'the scheduled dispatch finds nothing left to send');
  });
});
