import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  AwsSqsDriver,
  AwsSqsSdk,
  createAwsSqsDriver,
  isSqsClientLike,
  loadAwsSqsSdk,
  SqsClientLike,
} from '../driver';

interface SentCommand {
  name: string;
  input: Record<string, unknown>;
  options?: { abortSignal?: AbortSignal };
}

/** A client that records every command and answers from a queue of outputs. */
function recordingClient(outputs: unknown[] = []): SqsClientLike & { sent: SentCommand[]; destroyed: number } {
  const client = {
    sent: [] as SentCommand[],
    destroyed: 0,
    async send(command: unknown, options?: { abortSignal?: AbortSignal }): Promise<unknown> {
      const { constructor, input } = command as { constructor: { name: string }; input: Record<string, unknown> };
      client.sent.push(options ? { name: constructor.name, input, options } : { name: constructor.name, input });
      return outputs.shift() ?? {};
    },
    destroy(): void {
      client.destroyed += 1;
    },
  };
  return client;
}

describe('createAwsSqsDriver', () => {
  it('drives a supplied client with the SDK commands, and never destroys it', async () => {
    const client = recordingClient([{ Messages: [{ MessageId: 'm1', ReceiptHandle: 'h1', Body: 'b' }] }]);
    const driver = createAwsSqsDriver(client, 'default');
    assert.equal(driver.client, client);

    const signal = new AbortController().signal;
    const messages = await driver.receiveMessages(
      { queueUrl: 'https://q', maxMessages: 5, waitTimeSeconds: 20 },
      { signal },
    );

    assert.deepEqual(messages, [{ MessageId: 'm1', ReceiptHandle: 'h1', Body: 'b' }]);
    assert.deepEqual(client.sent, [
      {
        name: 'ReceiveMessageCommand',
        input: {
          QueueUrl: 'https://q',
          MaxNumberOfMessages: 5,
          WaitTimeSeconds: 20,
          MessageSystemAttributeNames: ['All'],
          MessageAttributeNames: ['All'],
        },
        options: { abortSignal: signal },
      },
    ]);
    driver.destroy();
    assert.equal(client.destroyed, 0);
  });

  it('creates and owns an SQSClient from a configuration', () => {
    const driver = createAwsSqsDriver({ region: 'us-east-1', endpoint: 'http://127.0.0.1:1' }, 'default');
    assert.equal((driver.client as { constructor: { name: string } }).constructor.name, 'SQSClient');
    driver.destroy();
  });
});

describe('AwsSqsDriver', () => {
  const sdk = loadAwsSqsSdk();

  it('passes the optional receive fields and answers an empty receive with no messages', async () => {
    const client = recordingClient();
    const driver = new AwsSqsDriver(sdk, client, false);
    const messages = await driver.receiveMessages({
      queueUrl: 'https://q.fifo',
      maxMessages: 1,
      waitTimeSeconds: 1,
      visibilityTimeout: 30,
      receiveRequestAttemptId: 'attempt-1',
    });
    assert.deepEqual(messages, []);
    assert.equal(client.sent[0].options, undefined);
    assert.equal(client.sent[0].input.VisibilityTimeout, 30);
    assert.equal(client.sent[0].input.ReceiveRequestAttemptId, 'attempt-1');
  });

  it('maps delete and visibility batches, per entry', async () => {
    const client = recordingClient([
      {
        Successful: [{ Id: '0' }, {}],
        Failed: [{ Id: '1', Code: 'ReceiptHandleIsInvalid', Message: 'bad', SenderFault: true }, {}],
      },
      {},
    ]);
    const driver = new AwsSqsDriver(sdk, client, false);

    const deleted = await driver.deleteMessages('https://q', [
      { id: '0', receiptHandle: 'h0' },
      { id: '1', receiptHandle: 'h1' },
    ]);
    const changed = await driver.changeVisibility('https://q', [{ id: '0', receiptHandle: 'h0', visibilityTimeout: 0 }]);

    assert.deepEqual(deleted, {
      successful: ['0', ''],
      failed: [
        { id: '1', code: 'ReceiptHandleIsInvalid', message: 'bad', senderFault: true },
        { id: '', code: 'Unknown', senderFault: false },
      ],
    });
    assert.deepEqual(changed, { successful: [], failed: [] });
    assert.deepEqual(client.sent[0], {
      name: 'DeleteMessageBatchCommand',
      input: {
        QueueUrl: 'https://q',
        Entries: [
          { Id: '0', ReceiptHandle: 'h0' },
          { Id: '1', ReceiptHandle: 'h1' },
        ],
      },
    });
    assert.deepEqual(client.sent[1], {
      name: 'ChangeMessageVisibilityBatchCommand',
      input: { QueueUrl: 'https://q', Entries: [{ Id: '0', ReceiptHandle: 'h0', VisibilityTimeout: 0 }] },
    });
  });

  it('maps a send batch with every optional field', async () => {
    const client = recordingClient([
      {
        Successful: [{ Id: '0', MessageId: 'm0', SequenceNumber: '1' }, { Id: '1', MessageId: 'm1' }, {}],
        Failed: [{ Id: '2', Code: 'InvalidMessageContents', SenderFault: true }],
      },
      {},
    ]);
    const driver = new AwsSqsDriver(sdk, client, false);
    const attributes = { type: { DataType: 'String', StringValue: 'x' } };

    const outcome = await driver.sendMessages('https://q', [
      {
        id: '0',
        body: 'a',
        delaySeconds: 5,
        messageAttributes: attributes,
        traceHeader: 'Root=1-abc',
        messageGroupId: 'g',
        messageDeduplicationId: 'd',
      },
      { id: '1', body: 'b' },
    ]);
    const empty = await driver.sendMessages('https://q', [{ id: '0', body: 'c' }]);

    assert.deepEqual(outcome, {
      successful: [
        { id: '0', messageId: 'm0', sequenceNumber: '1' },
        { id: '1', messageId: 'm1' },
        { id: '', messageId: '' },
      ],
      failed: [{ id: '2', code: 'InvalidMessageContents', senderFault: true }],
    });
    assert.deepEqual(empty, { successful: [], failed: [] });
    assert.deepEqual(client.sent[0].input, {
      QueueUrl: 'https://q',
      Entries: [
        {
          Id: '0',
          MessageBody: 'a',
          DelaySeconds: 5,
          MessageAttributes: attributes,
          MessageSystemAttributes: { AWSTraceHeader: { DataType: 'String', StringValue: 'Root=1-abc' } },
          MessageGroupId: 'g',
          MessageDeduplicationId: 'd',
        },
        { Id: '1', MessageBody: 'b' },
      ],
    });
  });

  it('resolves a queue URL, cross-account when asked, and fails without one', async () => {
    const client = recordingClient([{ QueueUrl: 'https://q/orders' }, { QueueUrl: 'https://q/other' }, {}]);
    const driver = new AwsSqsDriver(sdk, client, false);
    assert.equal(await driver.getQueueUrl('orders'), 'https://q/orders');
    assert.equal(await driver.getQueueUrl('other', '210987654321'), 'https://q/other');
    await assert.rejects(driver.getQueueUrl('missing'), /no URL for the queue "missing"/);
    assert.deepEqual(client.sent[0].input, { QueueName: 'orders' });
    assert.deepEqual(client.sent[1].input, { QueueName: 'other', QueueOwnerAWSAccountId: '210987654321' });
  });

  it('reads queue attributes, empty when SQS returns none', async () => {
    const client = recordingClient([{ Attributes: { VisibilityTimeout: '30' } }, {}]);
    const driver = new AwsSqsDriver(sdk, client, false);
    assert.deepEqual(await driver.getQueueAttributes('https://q', ['VisibilityTimeout']), { VisibilityTimeout: '30' });
    assert.deepEqual(await driver.getQueueAttributes('https://q', ['RedrivePolicy']), {});
    assert.deepEqual(client.sent[0], {
      name: 'GetQueueAttributesCommand',
      input: { QueueUrl: 'https://q', AttributeNames: ['VisibilityTimeout'] },
    });
  });

  it('destroys an owned client, even one without destroy()', () => {
    const owned = recordingClient();
    new AwsSqsDriver(sdk, owned, true).destroy();
    assert.equal(owned.destroyed, 1);
    new AwsSqsDriver(sdk, { send: async () => ({}) }, true).destroy();
  });
});

describe('isSqsClientLike', () => {
  it('recognizes anything with send()', () => {
    assert.equal(isSqsClientLike({ send: () => undefined }), true);
    assert.equal(isSqsClientLike({ region: 'us-east-1' }), false);
    assert.equal(isSqsClientLike(null), false);
    assert.equal(isSqsClientLike('client'), false);
  });
});

describe('loadAwsSqsSdk', () => {
  it('loads the installed SDK', () => {
    const sdk: AwsSqsSdk = loadAwsSqsSdk();
    assert.equal(typeof sdk.SQSClient, 'function');
  });

  it('says to install the peer when it is missing', () => {
    const missing = Object.assign(new Error('Cannot find module'), { code: 'MODULE_NOT_FOUND' });
    assert.throws(
      () =>
        loadAwsSqsSdk(() => {
          throw missing;
        }),
      (error: Error & { cause?: unknown }) =>
        /is not installed/.test(error.message) && error.cause === missing,
    );
  });

  it('reports a peer that is installed but fails to load', () => {
    assert.throws(
      () =>
        loadAwsSqsSdk(() => {
          throw new Error('setCredentialFeature is not a function');
        }),
      /installed but failed to load: setCredentialFeature is not a function/,
    );
    assert.throws(
      () =>
        loadAwsSqsSdk(() => {
          throw null;
        }),
      /failed to load: null/,
    );
  });
});
