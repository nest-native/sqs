import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { after, before, describe, it } from 'node:test';
import { Injectable, Logger } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { createAwsSqsDriver, SqsDriver } from '../../driver';
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

  async function start(providers: unknown[], queuesConfig: Record<string, string>): Promise<TestingModule> {
    const moduleRef = await Test.createTestingModule({
      imports: [
        SqsModule.forRoot({
          client: clientConfig,
          queues: queuesConfig,
          consumerDefaults: { waitTimeSeconds: 1 },
          retryBackoff: { initialDelaySeconds: 1, jitter: 'none' },
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
