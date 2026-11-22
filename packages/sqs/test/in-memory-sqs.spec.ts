import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { systemClock } from '../clock';
import { InMemorySqs, InMemorySqsError } from '../testing/in-memory-sqs';
import { VirtualClock } from '../testing/virtual-clock';
import { flush } from './helpers';

function setup(options: ConstructorParameters<typeof InMemorySqs>[0] = {}) {
  const clock = new VirtualClock(0);
  const sqs = new InMemorySqs({ clock, ...options });
  return { clock, sqs, driver: sqs.createDriver() };
}

async function receive(sqs: InMemorySqs, queue: string, overrides: { visibilityTimeout?: number } = {}) {
  return sqs.createDriver().receiveMessages({ queueUrl: sqs.queueUrl(queue), maxMessages: 10, waitTimeSeconds: 0, ...overrides });
}

describe('InMemorySqs queues', () => {
  it('creates standard queues with SQS-shaped URLs and ARNs, once', () => {
    const { sqs } = setup({ region: 'eu-west-1', accountId: '123456789012', endpoint: 'http://localhost:9324/' });
    const url = sqs.createQueue('orders');
    assert.equal(url, 'http://localhost:9324/123456789012/orders');
    assert.equal(sqs.createQueue('orders', { VisibilityTimeout: '5' }), url);
    assert.equal(sqs.queueArn('orders'), 'arn:aws:sqs:eu-west-1:123456789012:orders');
    assert.equal(new InMemorySqs().createQueue('q'), 'http://sqs.in-memory.test/000000000000/q');
  });

  it('refuses invalid names, FIFO queues and invalid attributes', () => {
    const { sqs } = setup();
    assert.throws(() => sqs.createQueue('bad name'), { name: 'InvalidParameterValue', message: /not a valid queue name/ });
    assert.throws(() => sqs.createQueue('ledger.fifo'), /FIFO queues are not modeled/);
    assert.throws(() => sqs.createQueue('q', { Policy: '{}' } as never), { name: 'InvalidAttributeName' });
    for (const attributes of [
      { VisibilityTimeout: '43201' },
      { DelaySeconds: '-1' },
      { MaximumMessageSize: '512' },
      { VisibilityTimeout: 'ten' },
      { RedrivePolicy: 'not json' },
      { RedrivePolicy: '{"maxReceiveCount":3}' },
      { RedrivePolicy: '{"deadLetterTargetArn":"arn","maxReceiveCount":0}' },
      { RedrivePolicy: '{"deadLetterTargetArn":"arn","maxReceiveCount":1001}' },
      { RedrivePolicy: '{"deadLetterTargetArn":"arn","maxReceiveCount":"x"}' },
    ]) {
      assert.throws(() => sqs.createQueue('q', attributes), { name: 'InvalidAttributeValue' });
    }
    assert.throws(() => sqs.queueUrl('missing'), { name: 'QueueDoesNotExist' });
  });

  it('resolves names, refuses another account, and creates on demand when asked', async () => {
    const { sqs, driver } = setup();
    sqs.createQueue('orders');
    assert.equal(await driver.getQueueUrl('orders', '000000000000'), sqs.queueUrl('orders'));
    await assert.rejects(driver.getQueueUrl('orders', '210987654321'), { name: 'QueueDoesNotExist' });
    await assert.rejects(driver.getQueueUrl('nope'), { name: 'QueueDoesNotExist' });
    await assert.rejects(driver.sendMessages('http://sqs.in-memory.test/000000000000/nope', [{ id: '0', body: 'x' }]), {
      name: 'QueueDoesNotExist',
    });

    const auto = setup({ autoCreateQueues: true });
    assert.equal(await auto.driver.getQueueUrl('fresh'), auto.sqs.queueUrl('fresh'));
  });

  it('reports the attributes it is asked for, all of them for All, and refuses unknown names', async () => {
    const { sqs, driver, clock } = setup();
    const url = sqs.createQueue('orders', { VisibilityTimeout: '10', DelaySeconds: '0' });
    sqs.sendMessage('orders', 'visible');
    sqs.sendMessage('orders', 'delayed', { delaySeconds: 60 });
    await receive(sqs, 'orders');
    sqs.sendMessage('orders', 'waiting');

    assert.deepEqual(await driver.getQueueAttributes(url, ['VisibilityTimeout', 'RedrivePolicy']), { VisibilityTimeout: '10' });
    assert.deepEqual(await driver.getQueueAttributes(url, ['All']), {
      VisibilityTimeout: '10',
      DelaySeconds: '0',
      MaximumMessageSize: '1048576',
      QueueArn: 'arn:aws:sqs:us-east-1:000000000000:orders',
      ApproximateNumberOfMessages: '1',
      ApproximateNumberOfMessagesNotVisible: '1',
      ApproximateNumberOfMessagesDelayed: '1',
    });
    await assert.rejects(driver.getQueueAttributes(url, ['Nope']), { name: 'InvalidAttributeName' });
    await clock.advance(0);
  });
});

describe('InMemorySqs messages', () => {
  it('reports every stored message and its state', async () => {
    const { sqs } = setup();
    sqs.createQueue('orders');
    const id = sqs.sendMessage(sqs.queueUrl('orders'), 'hello', { attributes: { type: 'greeting' } });
    assert.deepEqual(sqs.messages('orders'), [
      {
        messageId: id,
        body: 'hello',
        attributes: { type: { DataType: 'String', StringValue: 'greeting' } },
        receiveCount: 0,
        visible: true,
        inFlight: false,
      },
    ]);
    const [message] = await receive(sqs, 'orders');
    assert.deepEqual(message.MessageAttributes, { type: { DataType: 'String', StringValue: 'greeting' } });
    assert.match(message.MD5OfBody as string, /^[0-9a-f]{32}$/);
    assert.equal(message.Attributes?.SentTimestamp, '0');
    assert.equal(sqs.messages('orders')[0].inFlight, true);
    assert.throws(() => sqs.sendMessage('orders', ''), { name: 'InvalidMessageContents' });
  });

  it('refuses each malformed receive', async () => {
    const { sqs, driver } = setup();
    const queueUrl = sqs.createQueue('orders');
    for (const request of [
      { maxMessages: 0, waitTimeSeconds: 0 },
      { maxMessages: 11, waitTimeSeconds: 0 },
      { maxMessages: 1.5, waitTimeSeconds: 0 },
      { maxMessages: 1, waitTimeSeconds: 21 },
      { maxMessages: 1, waitTimeSeconds: -1 },
      { maxMessages: 1, waitTimeSeconds: 0.5 },
      { maxMessages: 1, waitTimeSeconds: 0, visibilityTimeout: 43_201 },
      { maxMessages: 1, waitTimeSeconds: 0, visibilityTimeout: -1 },
      { maxMessages: 1, waitTimeSeconds: 0, visibilityTimeout: 0.5 },
      { maxMessages: 1, waitTimeSeconds: 0, receiveRequestAttemptId: 'x' },
    ]) {
      await assert.rejects(driver.receiveMessages({ queueUrl, ...request }), { name: 'InvalidParameterValue' });
    }
  });

  it('long-polls until a message is sent, becomes visible, or the wait ends', async () => {
    const { sqs, driver, clock } = setup();
    const queueUrl = sqs.createQueue('orders');
    const waiting = driver.receiveMessages({ queueUrl, maxMessages: 1, waitTimeSeconds: 20 });
    await flush();
    sqs.sendMessage('orders', 'arrived');
    assert.equal((await waiting)[0].Body, 'arrived');

    sqs.sendMessage('orders', 'later', { delaySeconds: 5 });
    const delayed = driver.receiveMessages({ queueUrl, maxMessages: 1, waitTimeSeconds: 20 });
    await clock.advance(5_000);
    assert.equal((await delayed)[0].Body, 'later');

    const empty = driver.receiveMessages({ queueUrl, maxMessages: 1, waitTimeSeconds: 20 });
    await clock.advance(20_000);
    assert.deepEqual(await empty, []);
  });

  it('rejects an aborted receive, before and during the wait', async () => {
    const { sqs, driver } = setup();
    const queueUrl = sqs.createQueue('orders');
    const before = new AbortController();
    before.abort();
    await assert.rejects(driver.receiveMessages({ queueUrl, maxMessages: 1, waitTimeSeconds: 0 }, { signal: before.signal }), {
      name: 'AbortError',
    });
    const during = new AbortController();
    const waiting = driver.receiveMessages({ queueUrl, maxMessages: 1, waitTimeSeconds: 20 }, { signal: during.signal });
    await flush();
    during.abort();
    await assert.rejects(waiting, { name: 'AbortError' });
  });

  it('carries the trace header and a per-receive visibility', async () => {
    const { sqs, driver, clock } = setup();
    const queueUrl = sqs.createQueue('orders');
    await driver.sendMessages(queueUrl, [{ id: '0', body: 'traced', traceHeader: 'Root=1-abc' }]);
    const [message] = await receive(sqs, 'orders', { visibilityTimeout: 2 });
    assert.equal(message.Attributes?.AWSTraceHeader, 'Root=1-abc');
    assert.equal(message.Attributes?.ApproximateFirstReceiveTimestamp, '0');
    await clock.advance(2_000);
    assert.equal((await receive(sqs, 'orders')).length, 1);
  });
});

describe('InMemorySqs settling', () => {
  it('deletes only with the current receipt handle; an older one succeeds and does nothing', async () => {
    const { sqs, driver, clock } = setup();
    const queueUrl = sqs.createQueue('orders', { VisibilityTimeout: '1' });
    sqs.sendMessage('orders', 'x');
    const [first] = await receive(sqs, 'orders');
    await clock.advance(1_000);
    const [second] = await receive(sqs, 'orders');

    assert.deepEqual(await driver.deleteMessages(queueUrl, [{ id: 'old', receiptHandle: first.ReceiptHandle as string }]), {
      successful: ['old'],
      failed: [],
    });
    assert.equal(sqs.messages('orders').length, 1);

    const unknown = await driver.deleteMessages(queueUrl, [
      { id: 'u', receiptHandle: 'never-issued' },
      { id: 'n', receiptHandle: second.ReceiptHandle as string },
    ]);
    assert.deepEqual(unknown.successful, ['n']);
    assert.equal(unknown.failed[0].code, 'ReceiptHandleIsInvalid');
    assert.equal(sqs.messages('orders').length, 0);
    assert.deepEqual(
      await driver.deleteMessages(queueUrl, [{ id: 'again', receiptHandle: second.ReceiptHandle as string }]),
      { successful: ['again'], failed: [] },
      'deleting a deleted message again succeeds',
    );
  });

  it('refuses a receipt handle from another queue', async () => {
    const { sqs, driver } = setup();
    sqs.createQueue('a');
    const otherUrl = sqs.createQueue('b');
    sqs.sendMessage('a', 'x');
    const [message] = await receive(sqs, 'a');
    const outcome = await driver.deleteMessages(otherUrl, [{ id: '0', receiptHandle: message.ReceiptHandle as string }]);
    assert.equal(outcome.failed[0].code, 'ReceiptHandleIsInvalid');
  });

  it('changes the visibility of a message in flight only, within 12 hours of its receive', async () => {
    const { sqs, driver, clock } = setup();
    const queueUrl = sqs.createQueue('orders', { VisibilityTimeout: '43200' });
    sqs.sendMessage('orders', 'x');
    const [message] = await receive(sqs, 'orders');
    const handle = message.ReceiptHandle as string;
    const change = (visibilityTimeout: number, receiptHandle = handle) =>
      driver.changeVisibility(queueUrl, [{ id: '0', receiptHandle, visibilityTimeout }]);

    assert.equal((await change(-1)).failed[0].code, 'InvalidParameterValue');
    assert.equal((await change(1.5)).failed[0].code, 'InvalidParameterValue');
    assert.equal((await change(30, 'never-issued')).failed[0].code, 'ReceiptHandleIsInvalid');

    await clock.advance(43_100_000);
    assert.match((await change(200)).failed[0].message as string, /12 hours/);
    assert.deepEqual((await change(50)).successful, ['0']);

    await clock.advance(50_000);
    assert.equal((await change(10)).failed[0].code, 'MessageNotInflight', 'its visibility expired');

    const [again] = await receive(sqs, 'orders');
    assert.equal((await change(10)).failed[0].code, 'MessageNotInflight', 'an older receipt handle');
    await driver.deleteMessages(queueUrl, [{ id: '0', receiptHandle: again.ReceiptHandle as string }]);
    assert.equal((await change(10, again.ReceiptHandle as string)).failed[0].code, 'ReceiptHandleIsInvalid');
  });

  it('refuses malformed batches as SQS does', async () => {
    const { sqs, driver } = setup();
    const queueUrl = sqs.createQueue('orders');
    await assert.rejects(driver.deleteMessages(queueUrl, []), { name: 'EmptyBatchRequest' });
    await assert.rejects(
      driver.deleteMessages(
        queueUrl,
        Array.from({ length: 11 }, (_, index) => ({ id: String(index), receiptHandle: 'h' })),
      ),
      { name: 'TooManyEntriesInBatchRequest' },
    );
    await assert.rejects(driver.changeVisibility(queueUrl, [{ id: 'not valid!', receiptHandle: 'h', visibilityTimeout: 0 }]), {
      name: 'InvalidBatchEntryId',
    });
    await assert.rejects(
      driver.sendMessages(queueUrl, [
        { id: 'x', body: 'a' },
        { id: 'x', body: 'b' },
      ]),
      { name: 'BatchEntryIdsNotDistinct' },
    );
    await assert.rejects(
      driver.sendMessages(queueUrl, [
        { id: 'a', body: 'x'.repeat(600_000) },
        { id: 'b', body: 'x'.repeat(600_000) },
      ]),
      { name: 'BatchRequestTooLong' },
    );
  });

  it('refuses each malformed message, entry by entry', async () => {
    const { sqs, driver } = setup();
    const queueUrl = sqs.createQueue('orders', { MaximumMessageSize: '1024' });
    const manyAttributes = Object.fromEntries(
      Array.from({ length: 11 }, (_, index) => [`a${index}`, { DataType: 'String', StringValue: 'x' }]),
    );
    const outcome = await driver.sendMessages(queueUrl, [
      { id: 'empty', body: '' },
      { id: 'control', body: String.fromCharCode(1) },
      { id: 'attributes', body: 'x', messageAttributes: manyAttributes },
      { id: 'large', body: 'x'.repeat(1_025) },
      { id: 'delay', body: 'x', delaySeconds: 901 },
      { id: 'fraction', body: 'x', delaySeconds: 0.5 },
      { id: 'dedup', body: 'x', messageDeduplicationId: 'd' },
      { id: 'grouped', body: 'x', messageGroupId: 'tenant-1' },
    ]);
    assert.deepEqual(
      outcome.failed.map(failure => `${failure.id}:${failure.code}`),
      [
        'empty:InvalidMessageContents',
        'control:InvalidMessageContents',
        'attributes:InvalidParameterValue',
        'large:InvalidParameterValue',
        'delay:InvalidParameterValue',
        'fraction:InvalidParameterValue',
        'dedup:InvalidParameterValue',
      ],
    );
    assert.deepEqual(
      outcome.successful.map(success => success.id),
      ['grouped'],
      'a group id on a standard queue is fair queueing, which SQS accepts',
    );
  });
});

describe('InMemorySqs redrive', () => {
  it('moves a message to its dead-letter queue after maxReceiveCount receives, marking its source', async () => {
    const { sqs } = setup();
    sqs.createQueue('orders-dlq');
    sqs.createQueue('orders', {
      VisibilityTimeout: '0',
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: sqs.queueArn('orders-dlq'), maxReceiveCount: '2' }),
    });
    sqs.sendMessage('orders', 'poison');
    assert.equal((await receive(sqs, 'orders')).length, 1);
    assert.equal((await receive(sqs, 'orders')).length, 1);
    assert.deepEqual(await receive(sqs, 'orders'), []);

    const [dead] = sqs.messages('orders-dlq');
    assert.equal(dead.body, 'poison');
    assert.equal(dead.receiveCount, 0);
    assert.equal(dead.deadLetterSourceArn, sqs.queueArn('orders'));
    const [received] = await receive(sqs, 'orders-dlq');
    assert.equal(received.Attributes?.DeadLetterQueueSourceArn, sqs.queueArn('orders'));
  });

  it('keeps delivering when the dead-letter queue does not exist', async () => {
    const { sqs } = setup();
    sqs.createQueue('orders', {
      VisibilityTimeout: '0',
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: 'arn:aws:sqs:us-east-1:000000000000:gone', maxReceiveCount: 1 }),
    });
    sqs.sendMessage('orders', 'x');
    await receive(sqs, 'orders');
    assert.equal((await receive(sqs, 'orders')).length, 1);
  });
});

describe('InMemorySqs permissions and idleness', () => {
  it('refuses a denied operation with AccessDenied until it is allowed again', async () => {
    const { sqs, driver } = setup();
    const queueUrl = sqs.createQueue('orders');
    sqs.deny('GetQueueAttributes');
    await assert.rejects(driver.getQueueAttributes(queueUrl, ['All']), (error: unknown) => {
      assert.ok(error instanceof InMemorySqsError);
      assert.equal(error.name, 'AccessDenied');
      assert.equal(error.$fault, 'client');
      return true;
    });
    sqs.allow('GetQueueAttributes');
    await driver.getQueueAttributes(queueUrl, ['All']);
    driver.destroy();
    assert.equal(driver.client, sqs);
  });

  it('is idle when every polled queue has nothing visible and nothing in flight', async () => {
    const { sqs, driver } = setup();
    const queueUrl = sqs.createQueue('orders');
    sqs.createQueue('unpolled');
    sqs.sendMessage('unpolled', 'ignored: nothing polls this queue');
    await sqs.idle();

    sqs.sendMessage('orders', 'x');
    const [message] = await receive(sqs, 'orders');
    const idle = sqs.idle();
    await driver.deleteMessages(queueUrl, [{ id: '0', receiptHandle: message.ReceiptHandle as string }]);
    await idle;
  });

  it('says what is busy when it does not become idle in time', async () => {
    const { sqs } = setup();
    sqs.createQueue('orders');
    await receive(sqs, 'orders');
    sqs.sendMessage('orders', 'x');
    await assert.rejects(sqs.idle({ timeoutMs: 10 }), /not idle within 10 ms: orders: 1 visible, 0 in flight/);
  });

  it('runs on any clock', async () => {
    const sqs = new InMemorySqs({ clock: systemClock });
    sqs.createQueue('orders');
    sqs.sendMessage('orders', 'x');
    assert.equal((await receive(sqs, 'orders')).length, 1);
  });
});
