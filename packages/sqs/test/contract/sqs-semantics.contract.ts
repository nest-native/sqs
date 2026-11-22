import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { before, describe, it } from 'node:test';
import { SqsDriver, SqsReceivedMessage } from '../../driver';

/**
 * What a contract run needs from a backend: a driver, a way to create queues
 * (the package never creates them; tests do), and a way to let time pass —
 * virtual time for `InMemorySqs`, real time for a real endpoint.
 */
export interface SqsContractBackend {
  driver: SqsDriver;
  createQueue(name: string, attributes?: Record<string, string>): Promise<string>;
  wait(ms: number): Promise<void>;
}

/**
 * The SQS semantics this package depends on, asserted the same way against
 * every backend: `InMemorySqs` in the unit suite, and a real endpoint
 * (ElasticMQ in CI) in the gated suite. When the two disagree, the test double
 * is what is wrong — that is the point of running one spec on both.
 *
 * Visibility timeouts are a second or two, so a real run takes seconds. The
 * 5-minute end of the FIFO deduplication window is asserted on `InMemorySqs`
 * only (its unit spec), because a real endpoint would take 5 minutes.
 *
 * Where ElasticMQ and AWS disagree, these assertions state what both do; the
 * README of the package lists what ElasticMQ does differently.
 */
export function sqsSemanticsContract(
  label: string,
  setup: () => Promise<SqsContractBackend>,
  options: { skip?: boolean | string } = {},
): void {
  describe(`SQS semantics contract (${label})`, { skip: options.skip }, () => {
    let backend: SqsContractBackend;

    before(async () => {
      backend = await setup();
    });

    const name = (prefix: string): string => `${prefix}-${randomUUID().slice(0, 8)}`;

    async function receiveOne(queueUrl: string, visibilityTimeout?: number): Promise<SqsReceivedMessage> {
      const messages = await backend.driver.receiveMessages({ queueUrl, maxMessages: 1, waitTimeSeconds: 1, visibilityTimeout });
      assert.equal(messages.length, 1, 'expected one message');
      return messages[0];
    }

    async function receiveNone(queueUrl: string): Promise<void> {
      const messages = await backend.driver.receiveMessages({ queueUrl, maxMessages: 10, waitTimeSeconds: 0 });
      assert.deepEqual(messages, [], 'expected no visible message');
    }

    async function sendOne(queueUrl: string, body: string): Promise<string> {
      const outcome = await backend.driver.sendMessages(queueUrl, [{ id: '0', body }]);
      assert.deepEqual(outcome.failed, []);
      return outcome.successful[0].messageId;
    }

    function createFifoQueue(prefix: string, attributes: Record<string, string> = {}): Promise<string> {
      return backend.createQueue(`${name(prefix)}.fifo`, { FifoQueue: 'true', ...attributes });
    }

    /** A FIFO receive, with a `ReceiveRequestAttemptId` as the consumer sends one. */
    function receiveFifo(queueUrl: string, maxMessages = 10): Promise<SqsReceivedMessage[]> {
      return backend.driver.receiveMessages({
        queueUrl,
        maxMessages,
        waitTimeSeconds: 0,
        receiveRequestAttemptId: randomUUID(),
      });
    }

    async function sendFifo(queueUrl: string, body: string, groupId: string, deduplicationId?: string): Promise<string> {
      const outcome = await backend.driver.sendMessages(queueUrl, [
        { id: '0', body, messageGroupId: groupId, messageDeduplicationId: deduplicationId },
      ]);
      assert.deepEqual(outcome.failed, []);
      return outcome.successful[0].messageId;
    }

    const bodies = (messages: SqsReceivedMessage[]): (string | undefined)[] => messages.map(message => message.Body);

    function releaseAll(queueUrl: string, messages: SqsReceivedMessage[]) {
      return backend.driver.changeVisibility(
        queueUrl,
        messages.map((message, index) => ({
          id: String(index),
          receiptHandle: message.ReceiptHandle as string,
          visibilityTimeout: 0,
        })),
      );
    }

    it('resolves a queue URL by name, and refuses an unknown name with QueueDoesNotExist', async () => {
      const queue = name('contract-url');
      const url = await backend.createQueue(queue);
      assert.equal(await backend.driver.getQueueUrl(queue), url);
      await assert.rejects(backend.driver.getQueueUrl(name('contract-missing')), { name: 'QueueDoesNotExist' });
    });

    it('delivers the body and the message attributes, counting the first receive as 1', async () => {
      const url = await backend.createQueue(name('contract-deliver'));
      const sent = await backend.driver.sendMessages(url, [
        {
          id: 'first',
          body: '{"id":1}',
          messageAttributes: { type: { DataType: 'String', StringValue: 'OrderPlaced' } },
        },
      ]);
      assert.equal(sent.successful[0].id, 'first');

      const message = await receiveOne(url);
      assert.equal(message.MessageId, sent.successful[0].messageId);
      assert.equal(message.Body, '{"id":1}');
      assert.equal(message.Attributes?.ApproximateReceiveCount, '1');
      assert.equal(message.MessageAttributes?.type.StringValue, 'OrderPlaced');
      assert.ok(message.ReceiptHandle);
    });

    it('hides a received message for its visibility timeout, then redelivers it with a new receipt handle', async () => {
      const url = await backend.createQueue(name('contract-visibility'), { VisibilityTimeout: '1' });
      await sendOne(url, 'hidden');
      const first = await receiveOne(url);
      await receiveNone(url);

      await backend.wait(1_600);
      const second = await receiveOne(url);
      assert.equal(second.MessageId, first.MessageId);
      assert.equal(second.Attributes?.ApproximateReceiveCount, '2');
      assert.notEqual(second.ReceiptHandle, first.ReceiptHandle);
    });

    it('deletes a message with its current receipt handle', async () => {
      const url = await backend.createQueue(name('contract-delete'), { VisibilityTimeout: '1' });
      await sendOne(url, 'delete me');
      const message = await receiveOne(url);
      const outcome = await backend.driver.deleteMessages(url, [{ id: 'd', receiptHandle: message.ReceiptHandle as string }]);
      assert.deepEqual(outcome, { successful: ['d'], failed: [] });

      await backend.wait(1_600);
      await receiveNone(url);
    });

    it('counts a visibility change from the call, not from the receive', async () => {
      const url = await backend.createQueue(name('contract-change'));
      await sendOne(url, 'extend me');
      const message = await receiveOne(url, 30);

      await backend.wait(1_100);
      const changed = await backend.driver.changeVisibility(url, [
        { id: 'c', receiptHandle: message.ReceiptHandle as string, visibilityTimeout: 1 },
      ]);
      assert.deepEqual(changed, { successful: ['c'], failed: [] });
      // Counted from the receive, a 1-second timeout would already have expired.
      await receiveNone(url);

      await backend.wait(1_600);
      const again = await receiveOne(url);
      assert.equal(again.MessageId, message.MessageId);
    });

    it('makes a message visible at once when its visibility is changed to 0', async () => {
      const url = await backend.createQueue(name('contract-release'));
      await sendOne(url, 'release me');
      const message = await receiveOne(url, 30);
      await backend.driver.changeVisibility(url, [
        { id: 'r', receiptHandle: message.ReceiptHandle as string, visibilityTimeout: 0 },
      ]);
      const again = await receiveOne(url);
      assert.equal(again.MessageId, message.MessageId);
      assert.equal(again.Attributes?.ApproximateReceiveCount, '2');
    });

    it('accepts a send batch entry by entry', async () => {
      const url = await backend.createQueue(name('contract-batch'));
      const outcome = await backend.driver.sendMessages(url, [
        { id: 'a', body: 'one' },
        { id: 'b', body: 'two', delaySeconds: 0 },
      ]);
      assert.deepEqual(
        outcome.successful.map(entry => entry.id).sort(),
        ['a', 'b'],
      );
      assert.deepEqual(outcome.failed, []);
    });

    it('returns the queue attributes it is asked for', async () => {
      const dlq = await backend.createQueue(name('contract-attributes-dlq'));
      const [arn] = Object.values(await backend.driver.getQueueAttributes(dlq, ['QueueArn']));
      const url = await backend.createQueue(name('contract-attributes'), {
        VisibilityTimeout: '45',
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: 5 }),
      });
      const attributes = await backend.driver.getQueueAttributes(url, ['VisibilityTimeout', 'RedrivePolicy']);
      assert.equal(attributes.VisibilityTimeout, '45');
      const redrive = JSON.parse(attributes.RedrivePolicy as string) as { deadLetterTargetArn: string; maxReceiveCount: unknown };
      assert.equal(redrive.deadLetterTargetArn, arn);
      assert.equal(Number(redrive.maxReceiveCount), 5);
    });

    it('moves a message to the dead-letter queue once it has been received maxReceiveCount times', async () => {
      const dlq = await backend.createQueue(name('contract-dlq'));
      const arn = (await backend.driver.getQueueAttributes(dlq, ['QueueArn'])).QueueArn as string;
      const url = await backend.createQueue(name('contract-source'), {
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: 1 }),
      });
      const id = await sendOne(url, 'poison');
      const first = await receiveOne(url, 30);
      await backend.driver.changeVisibility(url, [
        { id: 'p', receiptHandle: first.ReceiptHandle as string, visibilityTimeout: 0 },
      ]);

      await receiveNone(url);
      const dead = await receiveOne(dlq);
      assert.equal(dead.MessageId, id);
      assert.equal(dead.Body, 'poison');
    });

    it('keeps a FIFO group in send order, and holds it while one of its messages is in flight', async () => {
      const url = await createFifoQueue('contract-fifo', { VisibilityTimeout: '30' });
      const sent = await backend.driver.sendMessages(
        url,
        ['g1', 'g2', 'g3'].map((body, index) => ({
          id: String(index),
          body,
          messageGroupId: 'g',
          messageDeduplicationId: body,
        })),
      );
      assert.deepEqual(sent.failed, []);

      const [g1, ...none] = await receiveFifo(url, 1);
      assert.deepEqual(none, []);
      assert.equal(g1.Body, 'g1', 'the oldest message of the group first');
      assert.equal(g1.Attributes?.MessageGroupId, 'g');
      assert.equal(g1.Attributes?.MessageDeduplicationId, 'g1');
      assert.match(String(g1.Attributes?.SequenceNumber), /^\d+$/);
      assert.deepEqual(await receiveFifo(url), [], 'the rest of the group waits while g1 is in flight');

      await sendFifo(url, 'h1', 'h', 'h1');
      const [h1] = await receiveFifo(url);
      assert.equal(h1.Body, 'h1', 'another group is served meanwhile');
      await sendFifo(url, 'h2', 'h', 'h2');
      assert.deepEqual(await receiveFifo(url), []);
      await backend.driver.deleteMessages(url, [{ id: '0', receiptHandle: h1.ReceiptHandle as string }]);
      assert.deepEqual(bodies(await receiveFifo(url)), ['h2'], 'deleting h1 lets h2 through');

      // A released head comes back first, with the rest of its group, in order, in one receive.
      await releaseAll(url, [g1]);
      const group = await receiveFifo(url);
      assert.deepEqual(bodies(group), ['g1', 'g2', 'g3']);
      const sequence = group.map(message => BigInt(message.Attributes?.SequenceNumber as string));
      assert.ok(sequence[0] < sequence[1] && sequence[1] < sequence[2], 'sequence numbers grow with the send order');

      // A retry: the head gets its backoff first, then the rest are released. The group waits for its head.
      await backend.driver.changeVisibility(url, [
        { id: '0', receiptHandle: group[0].ReceiptHandle as string, visibilityTimeout: 1 },
      ]);
      const released = await releaseAll(url, group.slice(1));
      assert.deepEqual(released.failed, []);
      assert.deepEqual(await receiveFifo(url), [], 'the released messages wait behind their head');
      await backend.wait(1_600);
      const again = await receiveFifo(url);
      assert.deepEqual(bodies(again), ['g1', 'g2', 'g3']);
      assert.deepEqual(
        again.map(message => message.Attributes?.ApproximateReceiveCount),
        ['3', '2', '2'],
      );
    });

    it('keeps FIFO messages in flight, and their group locked, for as long as their visibility is extended', async () => {
      const url = await createFifoQueue('contract-fifo-extend', { VisibilityTimeout: '1' });
      await sendFifo(url, 'e1', 'e', 'e1');
      await sendFifo(url, 'e2', 'e', 'e2');
      const held = await receiveFifo(url);
      assert.deepEqual(bodies(held), ['e1', 'e2']);

      // What the heartbeat does for a running message and the one waiting behind it.
      await backend.wait(500);
      const extended = await backend.driver.changeVisibility(
        url,
        held.map((message, index) => ({
          id: String(index),
          receiptHandle: message.ReceiptHandle as string,
          visibilityTimeout: 2,
        })),
      );
      assert.deepEqual(extended.failed, []);
      await backend.wait(1_100);
      assert.deepEqual(await receiveFifo(url), [], 'past their first visibility timeout, both are still in flight');
      await backend.wait(1_200);
      assert.deepEqual(bodies(await receiveFifo(url)), ['e1', 'e2'], 'and visible again once the extension ran out');
    });

    it('accepts a FIFO send that repeats a deduplication id within 5 minutes, and does not deliver it', async () => {
      const url = await createFifoQueue('contract-dedup');
      const first = await sendFifo(url, 'one', 'g', 'same');
      const repeated = await backend.driver.sendMessages(url, [
        { id: '0', body: 'two', messageGroupId: 'g', messageDeduplicationId: 'same' },
        { id: '1', body: 'three', messageGroupId: 'h', messageDeduplicationId: 'same' },
      ]);
      assert.deepEqual(repeated.failed, []);
      assert.deepEqual(
        repeated.successful.map(success => success.messageId),
        [first, first],
        "the first message's id, whatever the group",
      );

      const received = await receiveFifo(url);
      assert.deepEqual(bodies(received), ['one']);
      await backend.driver.deleteMessages(url, [{ id: '0', receiptHandle: received[0].ReceiptHandle as string }]);
      assert.equal(await sendFifo(url, 'four', 'g', 'same'), first);
      assert.deepEqual(await receiveFifo(url), [], 'the window outlives the message');
    });

    it('deduplicates by the SHA-256 of the body on a queue with content-based deduplication', async () => {
      const url = await createFifoQueue('contract-content', { ContentBasedDeduplication: 'true' });
      assert.deepEqual(await backend.driver.getQueueAttributes(url, ['FifoQueue', 'ContentBasedDeduplication']), {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'true',
      });
      const first = await sendFifo(url, 'body', 'g');
      const withAttributes = await backend.driver.sendMessages(url, [
        {
          id: '0',
          body: 'body',
          messageGroupId: 'g',
          messageAttributes: { type: { DataType: 'String', StringValue: 'other' } },
        },
      ]);
      assert.equal(withAttributes.successful[0].messageId, first, 'attributes are not part of the hash');
      await sendFifo(url, 'body', 'g', 'explicit');

      const received = await receiveFifo(url);
      assert.deepEqual(
        received.map(message => message.Attributes?.MessageDeduplicationId),
        [createHash('sha256').update('body').digest('hex'), 'explicit'],
        'an explicit deduplication id wins over the body',
      );
    });

    it('refuses a FIFO message without a group, without a deduplication id, or with its own delay', async () => {
      const url = await createFifoQueue('contract-fifo-rules');
      const outcome = await backend.driver.sendMessages(url, [
        { id: 'nogroup', body: 'x', messageDeduplicationId: 'd1' },
        { id: 'nodedup', body: 'x', messageGroupId: 'g' },
        { id: 'delay', body: 'x', messageGroupId: 'g', messageDeduplicationId: 'd2', delaySeconds: 5 },
        { id: 'badgroup', body: 'x', messageGroupId: 'a b', messageDeduplicationId: 'd3' },
        { id: 'baddedup', body: 'x', messageGroupId: 'g', messageDeduplicationId: 'd'.repeat(129) },
        { id: 'fine', body: 'x', messageGroupId: 'g', messageDeduplicationId: 'd4' },
      ]);
      assert.deepEqual(
        outcome.failed.map(failure => `${failure.id}:${failure.code}`).sort(),
        [
          'baddedup:InvalidParameterValue',
          'badgroup:InvalidParameterValue',
          'delay:InvalidParameterValue',
          'nodedup:InvalidParameterValue',
          'nogroup:MissingParameter',
        ],
      );
      assert.ok(outcome.failed.every(failure => failure.senderFault));
      assert.deepEqual(
        outcome.successful.map(success => success.id),
        ['fine'],
      );
    });
  });
}
