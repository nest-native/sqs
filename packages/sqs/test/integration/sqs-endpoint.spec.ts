import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { after, before, describe, it } from 'node:test';
import { Injectable, Logger } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { createAwsSqsDriver, SqsDriver, SqsReceivedMessage } from '../../driver';
import { SqsModuleOptions } from '../../interfaces';
import { SqsContext } from '../../sqs-context';
import { SqsHandler } from '../../sqs-handler.decorator';
import { SqsCtx, SqsMessage } from '../../sqs-params.decorators';
import { SqsProducerService } from '../../sqs-producer.service';
import { SqsModule } from '../../sqs.module';
import { sqsSemanticsContract } from '../contract/sqs-semantics.contract';
import { createRecordingLogger } from '../helpers';

/**
 * The gated suite: the semantics contract and a Nest application, against a
 * real SQS endpoint through the AWS SDK — ElasticMQ in CI, or anything
 * SQS-compatible locally (LocalStack, a real AWS account).
 *
 * It runs only when `SQS_ENDPOINT` is set, and skips cleanly otherwise, so the
 * unit suite and forks stay green without Docker. CI runs it through
 * `npm run test:integration:strict`, which fails if anything here skipped.
 *
 * Every queue name is unique per run, so repeated runs against one endpoint
 * never collide.
 */
const endpoint = process.env.SQS_ENDPOINT?.trim() ?? '';
const skip = endpoint.length === 0 ? 'SQS_ENDPOINT is not set' : false;

const clientConfig = {
  endpoint,
  region: process.env.AWS_REGION ?? 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
  },
};

interface SdkModule {
  SQSClient: new (config: Record<string, unknown>) => {
    send(command: unknown): Promise<{ QueueUrl?: string }>;
    destroy(): void;
  };
  CreateQueueCommand: new (input: Record<string, unknown>) => unknown;
}

/** Queue creation is the test's job: the package never provisions queues. */
function createQueueFunction(): { create(name: string, attributes?: Record<string, string>): Promise<string>; close(): void } {
  const sdk = require('@aws-sdk/client-sqs') as SdkModule;
  const client = new sdk.SQSClient(clientConfig);
  return {
    async create(name, attributes = {}) {
      const output = await client.send(new sdk.CreateQueueCommand({ QueueName: name, Attributes: attributes }));
      return output.QueueUrl as string;
    },
    close: () => client.destroy(),
  };
}

sqsSemanticsContract(
  `real endpoint ${endpoint || '(unset)'}`,
  async () => {
    const queues = createQueueFunction();
    return {
      driver: createAwsSqsDriver(clientConfig, 'default'),
      createQueue: (name, attributes) => queues.create(name, attributes),
      wait: ms => delay(ms),
    };
  },
  { skip },
);

describe('a Nest application on a real SQS endpoint', { skip }, () => {
  let queues: ReturnType<typeof createQueueFunction>;
  let driver: SqsDriver;
  const logs = createRecordingLogger();

  before(() => {
    queues = createQueueFunction();
    driver = createAwsSqsDriver(clientConfig, 'default');
    Logger.overrideLogger(logs);
  });

  after(() => {
    queues.close();
    driver.destroy();
  });

  async function waitFor(condition: () => boolean, timeoutMs = 15_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
      if (Date.now() > deadline) {
        throw new Error('timed out waiting for the condition');
      }
      await delay(50);
    }
  }

  async function start(
    providers: unknown[],
    queuesConfig: Record<string, string>,
    extra: Partial<SqsModuleOptions> = {},
  ): Promise<TestingModule> {
    const moduleRef = await Test.createTestingModule({
      imports: [
        SqsModule.forRoot({
          client: clientConfig,
          queues: queuesConfig,
          consumerDefaults: { waitTimeSeconds: 1 },
          retryBackoff: { initialDelaySeconds: 1, jitter: 'none' },
          ...extra,
        }),
      ],
      providers: providers as never[],
    })
      .setLogger(logs)
      .compile();
    await moduleRef.init();
    return moduleRef;
  }

  it('produces and consumes through the enhancer pipeline, and deletes what it handled', async () => {
    const name = `it-orders-${randomUUID().slice(0, 8)}`;
    const url = await queues.create(name);
    const received: unknown[] = [];

    @Injectable()
    class Orders {
      @SqsHandler('orders')
      placed(@SqsMessage() order: unknown): void {
        received.push(order);
      }
    }

    const moduleRef = await start([Orders], { orders: name });
    await moduleRef.get(SqsProducerService).send('orders', { body: { id: 1 }, attributes: { type: 'OrderPlaced' } });
    await waitFor(() => received.length === 1);
    assert.deepEqual(received, [{ id: 1 }]);
    await moduleRef.close();

    await delay(500);
    const left = await driver.receiveMessages({ queueUrl: url, maxMessages: 10, waitTimeSeconds: 0, visibilityTimeout: 0 });
    assert.deepEqual(left, [], 'the handled message was deleted');
  });

  it('retries a failure after the backoff, counting the receive', async () => {
    const name = `it-retry-${randomUUID().slice(0, 8)}`;
    await queues.create(name);
    const attempts: { count: number; at: number }[] = [];

    @Injectable()
    class Flaky {
      @SqsHandler('flaky')
      handle(@SqsCtx() context: SqsContext): void {
        attempts.push({ count: context.getReceiveCount(), at: Date.now() });
        if (attempts.length === 1) {
          throw new Error('downstream timeout');
        }
      }
    }

    const moduleRef = await start([Flaky], { flaky: name });
    await moduleRef.get(SqsProducerService).send('flaky', { body: 'x' });
    await waitFor(() => attempts.length === 2);
    assert.deepEqual(
      attempts.map(attempt => attempt.count),
      [1, 2],
    );
    assert.ok(attempts[1].at - attempts[0].at >= 900, 'redelivered no sooner than the 1 s backoff');
    await moduleRef.close();
  });

  it('keeps each FIFO group in order under concurrency 4, and a failure in one group does not hold the other', async () => {
    const name = `it-ledger-${randomUUID().slice(0, 8)}.fifo`;
    const url = await queues.create(name, { FifoQueue: 'true' });
    const perGroup = 5;
    const attempts: { message: string; receiveCount: number; startedAt: number; endedAt?: number; failed?: boolean }[] = [];
    const running = new Map<string, number>();
    let mostAtOnce = 0;
    let mostAtOnceInAGroup = 0;

    @Injectable()
    class Ledger {
      @SqsHandler('ledger', { concurrency: 4 })
      async post(@SqsMessage() entry: { group: string; seq: number }, @SqsCtx() context: SqsContext): Promise<void> {
        const attempt = { message: `${entry.group}${entry.seq}`, receiveCount: context.getReceiveCount(), startedAt: Date.now() };
        attempts.push(attempt);
        running.set(entry.group, (running.get(entry.group) ?? 0) + 1);
        mostAtOnceInAGroup = Math.max(mostAtOnceInAGroup, running.get(entry.group) as number);
        mostAtOnce = Math.max(mostAtOnce, [...running.values()].reduce((sum, count) => sum + count, 0));
        try {
          await delay(50);
          if (attempt.message === 'A2' && attempt.receiveCount === 1) {
            Object.assign(attempt, { failed: true });
            throw new Error('injected failure');
          }
        } finally {
          running.set(entry.group, (running.get(entry.group) as number) - 1);
          Object.assign(attempt, { endedAt: Date.now() });
        }
      }
    }

    const moduleRef = await start([Ledger], { ledger: name }, { retryBackoff: { initialDelaySeconds: 2, jitter: 'none' } });
    try {
      const messages = Array.from({ length: perGroup }, (_, index) =>
        ['A', 'B'].map(group => ({ body: { group, seq: index + 1 }, groupId: group, deduplicationId: `${group}${index + 1}` })),
      ).flat();
      await moduleRef.get(SqsProducerService).sendBatch('ledger', messages);
      const succeeded = () => attempts.filter(attempt => attempt.endedAt !== undefined && !attempt.failed);
      await waitFor(() => succeeded().length === perGroup * 2, 20_000);
    } finally {
      await moduleRef.close();
    }

    const ofGroup = (group: string, list: typeof attempts) => list.filter(attempt => attempt.message.startsWith(group));
    assert.deepEqual(
      ofGroup('A', attempts).map(attempt => attempt.message),
      ['A1', 'A2', 'A2', 'A3', 'A4', 'A5'],
      "A's order is kept: A2 is redelivered before A3 runs",
    );
    assert.deepEqual(
      ofGroup('B', attempts).map(attempt => `${attempt.message}#${attempt.receiveCount}`),
      ['B1#1', 'B2#1', 'B3#1', 'B4#1', 'B5#1'],
    );
    assert.equal(mostAtOnceInAGroup, 1, 'one message of a group at a time');
    assert.ok(mostAtOnce >= 2 && mostAtOnce <= 4, `the groups ran side by side, within the concurrency (${mostAtOnce})`);

    const [failure, retry] = ofGroup('A', attempts).filter(attempt => attempt.message === 'A2');
    const bAttempts = ofGroup('B', attempts);
    const lastB = bAttempts[bAttempts.length - 1];
    assert.ok((retry.startedAt - (failure.endedAt as number)) >= 1_800, 'A2 waited out its 2 s backoff');
    assert.ok((lastB.endedAt as number) < retry.startedAt, 'B finished while A2 waited: A did not delay B');

    const left = await driver.receiveMessages({
      queueUrl: url,
      maxMessages: 10,
      waitTimeSeconds: 0,
      receiveRequestAttemptId: randomUUID(),
    });
    assert.deepEqual(left, [], 'every message was deleted');
  });

  it('keeps a long FIFO message, and the one waiting behind it, from another consumer with the heartbeat', async () => {
    const name = `it-heartbeat-${randomUUID().slice(0, 8)}.fifo`;
    const url = await queues.create(name, { FifoQueue: 'true', VisibilityTimeout: '2' });
    const handled: { message: string; receiveCount: number; startedAt: number; endedAt?: number }[] = [];

    @Injectable()
    class Slow {
      @SqsHandler('slow', { concurrency: 2 })
      async handle(@SqsMessage() message: string, @SqsCtx() context: SqsContext): Promise<void> {
        const entry = { message, receiveCount: context.getReceiveCount(), startedAt: Date.now() };
        handled.push(entry);
        await delay(message === 'first' ? 6_000 : 1_000);
        Object.assign(entry, { endedAt: Date.now() });
      }
    }

    // Both are on the queue before the application polls, so its first receive takes both:
    // "first" runs, "second" waits behind it.
    const sent = await driver.sendMessages(url, [
      { id: '0', body: 'first', messageGroupId: 'g', messageDeduplicationId: 'first' },
      { id: '1', body: 'second', messageGroupId: 'g', messageDeduplicationId: 'second' },
    ]);
    assert.deepEqual(sent.failed, []);
    const stolen: SqsReceivedMessage[] = [];
    let polling = true;
    let rival: Promise<void> = Promise.resolve();
    const moduleRef = await start([Slow], { slow: name });
    try {
      await waitFor(() => handled.length === 1);
      assert.deepEqual(
        await driver.getQueueAttributes(url, ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']),
        { ApproximateNumberOfMessages: '0', ApproximateNumberOfMessagesNotVisible: '2' },
        'the application holds both messages',
      );

      // A second consumer long-polls the same queue for as long as they are processed.
      rival = (async () => {
        while (polling) {
          stolen.push(
            ...(await driver.receiveMessages({
              queueUrl: url,
              maxMessages: 10,
              waitTimeSeconds: 1,
              receiveRequestAttemptId: randomUUID(),
            })),
          );
        }
      })();
      await waitFor(() => handled.length === 2 && handled.every(entry => entry.endedAt !== undefined), 20_000);
    } finally {
      polling = false;
      await rival;
      await moduleRef.close();
    }

    assert.deepEqual(
      stolen.map(message => message.Body),
      [],
      'neither message became visible: the heartbeat extended the running one and the waiting one',
    );
    assert.deepEqual(
      handled.map(entry => `${entry.message}#${entry.receiveCount}`),
      ['first#1', 'second#1'],
    );
    assert.ok(handled[1].startedAt >= (handled[0].endedAt as number), '"second" ran after "first"');
    assert.ok(
      (handled[0].endedAt as number) - handled[0].startedAt >= 5_900,
      'the handler ran three times as long as the visibility timeout',
    );
  });

  it('fails the bootstrap when a handler names a queue that does not exist', async () => {
    @Injectable()
    class Missing {
      @SqsHandler('missing')
      handle(): void {}
    }

    await assert.rejects(
      start([Missing], { missing: `it-missing-${randomUUID().slice(0, 8)}` }),
      /Missing.handle: could not resolve the queue "missing"/,
    );
  });
});
