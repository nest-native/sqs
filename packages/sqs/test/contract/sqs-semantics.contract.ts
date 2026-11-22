import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
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
 * Visibility timeouts are a second or two, so a real run takes seconds.
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
  });
}
