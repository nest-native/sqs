import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
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

  it('refuses invalid names and invalid attributes', () => {
    const { sqs } = setup();
    assert.throws(() => sqs.createQueue('bad name'), { name: 'InvalidParameterValue', message: /not a valid queue name/ });
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

describe('InMemorySqs FIFO queues', () => {
  const FIFO = { FifoQueue: 'true' };

  async function receiveFifo(sqs: InMemorySqs, queue: string, maxMessages = 10, visibilityTimeout?: number) {
    const messages = await sqs.createDriver().receiveMessages({
      queueUrl: sqs.queueUrl(queue),
      maxMessages,
      waitTimeSeconds: 0,
      visibilityTimeout,
      receiveRequestAttemptId: 'attempt-1',
    });
    return messages;
  }

  const bodies = (messages: { Body?: string }[]) => messages.map(message => message.Body);

  it('creates FIFO queues, and refuses the names and attributes SQS refuses', async () => {
    const { sqs, driver } = setup({ autoCreateQueues: true });
    const url = sqs.createQueue('ledger.fifo', FIFO);
    assert.equal(url, 'http://sqs.in-memory.test/000000000000/ledger.fifo');
    assert.deepEqual(await driver.getQueueAttributes(url, ['FifoQueue', 'ContentBasedDeduplication']), {
      FifoQueue: 'true',
      ContentBasedDeduplication: 'false',
    });
    const dedup = sqs.createQueue('dedup.fifo', { FifoQueue: 'true', ContentBasedDeduplication: 'true' });
    assert.equal((await driver.getQueueAttributes(dedup, ['All'])).ContentBasedDeduplication, 'true');
    const standard = sqs.createQueue('plain', { FifoQueue: 'false' });
    assert.deepEqual(await driver.getQueueAttributes(standard, ['FifoQueue', 'ContentBasedDeduplication']), {});
    assert.equal(sqs.createQueue(`${'x'.repeat(75)}.fifo`, FIFO).endsWith('.fifo'), true);

    assert.throws(() => sqs.createQueue('orders.fifo'), {
      name: 'InvalidParameterValue',
      message: /not a valid queue name: .*\(a FIFO queue needs the FifoQueue attribute set to 'true'\)/,
    });
    for (const name of ['ledger', `${'x'.repeat(76)}.fifo`, 'le dger.fifo', '.fifo']) {
      assert.throws(() => sqs.createQueue(name, FIFO), { name: 'InvalidParameterValue', message: /not a valid FIFO queue name/ });
    }
    assert.throws(() => sqs.createQueue('q', { FifoQueue: 'yes' }), { name: 'InvalidAttributeValue' });
    assert.throws(() => sqs.createQueue('c.fifo', { FifoQueue: 'true', ContentBasedDeduplication: 'on' }), {
      name: 'InvalidAttributeValue',
    });
    assert.throws(() => sqs.createQueue('q', { ContentBasedDeduplication: 'true' }), {
      name: 'InvalidAttributeName',
      message: 'Unknown Attribute ContentBasedDeduplication.',
    });

    const auto = await driver.getQueueUrl('auto.fifo');
    assert.deepEqual(await driver.getQueueAttributes(auto, ['FifoQueue']), { FifoQueue: 'true' });
  });

  it('refuses each FIFO send SQS refuses, entry by entry, with its code', async () => {
    const { sqs, driver } = setup();
    const url = sqs.createQueue('ledger.fifo', FIFO);
    const outcome = await driver.sendMessages(url, [
      { id: 'nogroup', body: 'x', messageDeduplicationId: 'd1' },
      { id: 'nodedup', body: 'x', messageGroupId: 'g' },
      { id: 'delay', body: 'x', messageGroupId: 'g', messageDeduplicationId: 'd2', delaySeconds: 5 },
      { id: 'zerodelay', body: 'x', messageGroupId: 'g', messageDeduplicationId: 'd3', delaySeconds: 0 },
      { id: 'badgroup', body: 'x', messageGroupId: 'a b', messageDeduplicationId: 'd4' },
      { id: 'longgroup', body: 'x', messageGroupId: 'g'.repeat(129), messageDeduplicationId: 'd5' },
      { id: 'baddedup', body: 'x', messageGroupId: 'g', messageDeduplicationId: 'd'.repeat(129) },
      { id: 'empty', body: '', messageGroupId: 'g', messageDeduplicationId: 'd6' },
      { id: 'fine', body: 'x', messageGroupId: '!"#$%&()*+,-./:;<=>?@[]^_`{|}~', messageDeduplicationId: 'd7' },
    ]);
    assert.deepEqual(
      outcome.failed.map(failure => `${failure.id}:${failure.code}`),
      [
        'nogroup:MissingParameter',
        'nodedup:InvalidParameterValue',
        'delay:InvalidParameterValue',
        'zerodelay:InvalidParameterValue',
        'badgroup:InvalidParameterValue',
        'longgroup:InvalidParameterValue',
        'baddedup:InvalidParameterValue',
        'empty:InvalidMessageContents',
      ],
    );
    assert.match(String(outcome.failed[1].message), /either have ContentBasedDeduplication enabled or MessageDeduplicationId/);
    assert.deepEqual(
      outcome.successful.map(success => success.id),
      ['fine'],
    );
    assert.throws(() => sqs.sendMessage('ledger.fifo', 'x', { deduplicationId: 'd8' }), { name: 'MissingParameter' });

    const standard = sqs.createQueue('orders');
    const fair = await driver.sendMessages(standard, [{ id: 'bad', body: 'x', messageGroupId: 'a b' }]);
    assert.equal(fair.failed[0].code, 'InvalidParameterValue', 'a fair-queue group id follows the same rule');
  });

  it('deduplicates a send within 5 minutes by its id, even once the first message is gone', async () => {
    const { sqs, driver, clock } = setup();
    const url = sqs.createQueue('ledger.fifo', FIFO);
    const first = sqs.sendMessage('ledger.fifo', 'one', { groupId: 'g', deduplicationId: 'same' });
    const again = await driver.sendMessages(url, [
      { id: 'a', body: 'two', messageGroupId: 'g', messageDeduplicationId: 'same' },
      { id: 'b', body: 'three', messageGroupId: 'h', messageDeduplicationId: 'same' },
    ]);
    assert.deepEqual(
      again.successful.map(success => success.messageId),
      [first, first],
      'accepted, with the first message id, whatever the group',
    );
    assert.equal(again.successful[0].sequenceNumber, sqs.messages('ledger.fifo')[0].sequenceNumber);
    const [message] = await receiveFifo(sqs, 'ledger.fifo');
    assert.equal(message.Body, 'one');
    assert.equal(message.Attributes?.MessageDeduplicationId, 'same');
    await driver.deleteMessages(url, [{ id: '0', receiptHandle: message.ReceiptHandle as string }]);
    assert.equal(sqs.sendMessage('ledger.fifo', 'four', { groupId: 'g', deduplicationId: 'same' }), first);
    assert.deepEqual(sqs.messages('ledger.fifo'), []);

    await clock.advance(5 * 60 * 1000 - 1);
    sqs.sendMessage('ledger.fifo', 'five', { groupId: 'g', deduplicationId: 'same' });
    sqs.sendMessage('ledger.fifo', 'six', { groupId: 'g', deduplicationId: 'other' });
    assert.deepEqual(bodies(await receiveFifo(sqs, 'ledger.fifo')), ['six'], 'still within the window');

    await clock.advance(1);
    const fresh = sqs.sendMessage('ledger.fifo', 'seven', { groupId: 'h', deduplicationId: 'same' });
    assert.notEqual(fresh, first, 'the window closed 5 minutes after the first send');
    assert.equal(sqs.sendMessage('ledger.fifo', 'eight', { groupId: 'h', deduplicationId: 'other' }), sqs.messages('ledger.fifo')[0].messageId);
  });

  it('deduplicates by the SHA-256 of the body on a queue with content-based deduplication', async () => {
    const { sqs } = setup();
    sqs.createQueue('events.fifo', { FifoQueue: 'true', ContentBasedDeduplication: 'true' });
    const first = sqs.sendMessage('events.fifo', 'body', { groupId: 'g' });
    assert.equal(sqs.sendMessage('events.fifo', 'body', { groupId: 'g', attributes: { type: 'other' } }), first, 'attributes do not count');
    const explicit = sqs.sendMessage('events.fifo', 'body', { groupId: 'g', deduplicationId: 'explicit' });
    assert.notEqual(explicit, first, 'an explicit id wins over the body');
    const [state] = sqs.messages('events.fifo');
    assert.equal(state.deduplicationId, createHash('sha256').update('body').digest('hex'));
    assert.equal(sqs.messages('events.fifo').length, 2);
  });

  it('numbers every FIFO message with an increasing 20-digit sequence number, on the send and on the receive', async () => {
    const { sqs, driver } = setup();
    const url = sqs.createQueue('ledger.fifo', FIFO);
    const sent = await driver.sendMessages(url, [
      { id: 'a', body: 'one', messageGroupId: 'g', messageDeduplicationId: '1' },
      { id: 'b', body: 'two', messageGroupId: 'h', messageDeduplicationId: '2' },
    ]);
    const numbers = sent.successful.map(success => success.sequenceNumber as string);
    assert.ok(numbers.every(number => /^\d{20}$/.test(number)));
    assert.ok(BigInt(numbers[0]) < BigInt(numbers[1]));
    assert.ok(BigInt(numbers[1]) - BigInt(numbers[0]) > 1n, 'not consecutive, as SQS warns');

    const received = await receiveFifo(sqs, 'ledger.fifo');
    assert.deepEqual(
      received.map(message => [message.Body, message.Attributes?.MessageGroupId, message.Attributes?.SequenceNumber]),
      [
        ['one', 'g', numbers[0]],
        ['two', 'h', numbers[1]],
      ],
    );
    assert.deepEqual(
      sqs.messages('ledger.fifo').map(state => [state.groupId, state.deduplicationId, state.sequenceNumber]),
      [
        ['g', '1', numbers[0]],
        ['h', '2', numbers[1]],
      ],
    );
    const plain = sqs.createQueue('orders');
    const standard = await driver.sendMessages(plain, [{ id: 'a', body: 'x' }]);
    assert.equal(standard.successful[0].sequenceNumber, undefined);
    assert.equal('groupId' in sqs.messages('orders')[0], false);
  });

  it('returns as many messages of one group as it can, in order, before the next group', async () => {
    const { sqs } = setup();
    sqs.createQueue('ledger.fifo', FIFO);
    for (const [body, groupId] of [
      ['a1', 'a'],
      ['b1', 'b'],
      ['a2', 'a'],
      ['b2', 'b'],
      ['a3', 'a'],
    ]) {
      sqs.sendMessage('ledger.fifo', body, { groupId, deduplicationId: body });
    }
    assert.deepEqual(bodies(await receiveFifo(sqs, 'ledger.fifo', 2)), ['a1', 'a2']);
    assert.deepEqual(bodies(await receiveFifo(sqs, 'ledger.fifo')), ['b1', 'b2'], 'a3 waits behind a1 and a2 in flight');
    assert.deepEqual(await receiveFifo(sqs, 'ledger.fifo'), []);
  });

  it('holds a group behind its first invisible message, and serves the group again from its head', async () => {
    const { sqs, driver, clock } = setup();
    const url = sqs.createQueue('ledger.fifo', { FifoQueue: 'true', VisibilityTimeout: '30' });
    for (const body of ['g1', 'g2', 'g3']) {
      sqs.sendMessage('ledger.fifo', body, { groupId: 'g', deduplicationId: body });
    }
    const [g1] = await receiveFifo(sqs, 'ledger.fifo', 1);
    assert.equal(g1.Body, 'g1');
    assert.deepEqual(await receiveFifo(sqs, 'ledger.fifo'), [], 'g2 and g3 wait while g1 is in flight');
    sqs.sendMessage('ledger.fifo', 'h1', { groupId: 'h', deduplicationId: 'h1' });
    assert.deepEqual(bodies(await receiveFifo(sqs, 'ledger.fifo')), ['h1'], 'another group is served');

    await driver.changeVisibility(url, [{ id: '0', receiptHandle: g1.ReceiptHandle as string, visibilityTimeout: 0 }]);
    const held = await receiveFifo(sqs, 'ledger.fifo');
    assert.deepEqual(bodies(held), ['g1', 'g2', 'g3'], 'a released head comes back first, with the rest of its group');

    // The retry flow: the failed head gets its backoff, then the rest are released.
    await driver.changeVisibility(url, [{ id: '0', receiptHandle: held[0].ReceiptHandle as string, visibilityTimeout: 2 }]);
    await driver.changeVisibility(
      url,
      held.slice(1).map((message, index) => ({
        id: String(index),
        receiptHandle: message.ReceiptHandle as string,
        visibilityTimeout: 0,
      })),
    );
    assert.deepEqual(await receiveFifo(sqs, 'ledger.fifo'), [], 'the group waits for its head');
    await clock.advance(2_000);
    const again = await receiveFifo(sqs, 'ledger.fifo');
    assert.deepEqual(bodies(again), ['g1', 'g2', 'g3'], 'the head first, then the rest, in order');
    assert.deepEqual(
      again.map(message => message.Attributes?.ApproximateReceiveCount),
      ['3', '2', '2'],
    );

    // A released head is served even while a later message of its group is in flight.
    await driver.changeVisibility(url, [{ id: '0', receiptHandle: again[0].ReceiptHandle as string, visibilityTimeout: 0 }]);
    assert.deepEqual(bodies(await receiveFifo(sqs, 'ledger.fifo')), ['g1']);
  });

  it('holds a group behind a delayed message, and wakes a long poll when the group frees up', async () => {
    const { sqs, driver, clock } = setup();
    const url = sqs.createQueue('slow.fifo', { FifoQueue: 'true', DelaySeconds: '5' });
    sqs.sendMessage('slow.fifo', 'd1', { groupId: 'g', deduplicationId: 'd1' });
    assert.deepEqual(await receiveFifo(sqs, 'slow.fifo'), []);
    await clock.advance(5_000);
    const [d1] = await receiveFifo(sqs, 'slow.fifo');
    sqs.sendMessage('slow.fifo', 'd2', { groupId: 'g', deduplicationId: 'd2' });
    await clock.advance(5_000);

    const polling = driver.receiveMessages({
      queueUrl: url,
      maxMessages: 10,
      waitTimeSeconds: 20,
      receiveRequestAttemptId: 'poll',
    });
    await flush();
    await driver.deleteMessages(url, [{ id: '0', receiptHandle: d1.ReceiptHandle as string }]);
    assert.deepEqual(bodies(await polling), ['d2']);
  });

  it('checks ReceiveRequestAttemptId, which only FIFO queues take', async () => {
    const { sqs, driver } = setup();
    const fifo = sqs.createQueue('ledger.fifo', FIFO);
    const standard = sqs.createQueue('orders');
    const receive = (queueUrl: string, receiveRequestAttemptId: string) =>
      driver.receiveMessages({ queueUrl, maxMessages: 1, waitTimeSeconds: 0, receiveRequestAttemptId });
    await assert.rejects(receive(standard, 'x'), { name: 'InvalidParameterValue', message: /not valid for this queue type/ });
    await assert.rejects(receive(fifo, 'a b'), { name: 'InvalidParameterValue', message: /ReceiveRequestAttemptId can only include/ });
    await assert.rejects(receive(fifo, 'x'.repeat(129)), { name: 'InvalidParameterValue' });
    assert.deepEqual(await receive(fifo, randomUUID()), []);
    await assert.rejects(receive('http://sqs.in-memory.test/000000000000/missing.fifo', 'x'), { name: 'QueueDoesNotExist' });
  });

  it('moves the head of a group to its dead-letter queue, and serves the next one', async () => {
    const { sqs, driver } = setup();
    sqs.createQueue('ledger-dlq.fifo', FIFO);
    const url = sqs.createQueue('ledger.fifo', {
      FifoQueue: 'true',
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: sqs.queueArn('ledger-dlq.fifo'), maxReceiveCount: 1 }),
    });
    sqs.sendMessage('ledger.fifo', 'p1', { groupId: 'p', deduplicationId: 'p1' });
    sqs.sendMessage('ledger.fifo', 'p2', { groupId: 'p', deduplicationId: 'p2' });
    const [p1] = await receiveFifo(sqs, 'ledger.fifo', 1);
    await driver.changeVisibility(url, [{ id: '0', receiptHandle: p1.ReceiptHandle as string, visibilityTimeout: 0 }]);
    assert.deepEqual(bodies(await receiveFifo(sqs, 'ledger.fifo')), ['p2']);
    const [dead] = await receiveFifo(sqs, 'ledger-dlq.fifo');
    assert.equal(dead.Body, 'p1');
    assert.equal(dead.Attributes?.MessageGroupId, 'p');
  });

  it('is idle when the visible FIFO messages wait behind one that is invisible', async () => {
    const { sqs, driver } = setup();
    const url = sqs.createQueue('ledger.fifo', FIFO);
    sqs.sendMessage('ledger.fifo', 'g1', { groupId: 'g', deduplicationId: 'g1' });
    sqs.sendMessage('ledger.fifo', 'g2', { groupId: 'g', deduplicationId: 'g2' });
    const received = await receiveFifo(sqs, 'ledger.fifo');
    await driver.changeVisibility(url, [
      { id: '0', receiptHandle: received[0].ReceiptHandle as string, visibilityTimeout: 5 },
      { id: '1', receiptHandle: received[1].ReceiptHandle as string, visibilityTimeout: 0 },
    ]);
    assert.equal(sqs.messages('ledger.fifo')[1].visible, true);
    await sqs.idle();
    sqs.sendMessage('ledger.fifo', 'h1', { groupId: 'h', deduplicationId: 'h1' });
    await assert.rejects(sqs.idle({ timeoutMs: 10 }), /ledger.fifo: 1 visible, 0 in flight/);
  });
});
