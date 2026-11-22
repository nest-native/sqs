import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SqsReceivedMessage } from '../driver';
import { DEFAULT_SQS_CLIENT, SqsClients } from '../sqs-clients';
import { resolveConsumerOptions } from '../sqs-consumer-options';
import { SqsContext, SqsMessageControl } from '../sqs-context';
import { toDeadLetterMessage } from '../sqs-dead-letter';
import { defaultSqsErrorMapper } from '../sqs-error-mapping';
import { SqsQueueRegistry } from '../sqs-queue-registry';
import { createFakeDriver, FakeDriver } from './helpers';

const QUEUE = { key: 'orders', name: 'orders', url: 'https://q/orders', isFifo: false, client: 'default' };

function context(message: SqsReceivedMessage, control?: Partial<SqsMessageControl>): SqsContext {
  return new SqsContext(QUEUE, message, new AbortController().signal, {
    delete: async () => undefined,
    changeVisibility: async () => undefined,
    ...control,
  });
}

describe('SqsContext', () => {
  it('exposes the message as SQS returned it', () => {
    const bytes = new Uint8Array([1]);
    const ctx = context({
      MessageId: 'm1',
      ReceiptHandle: 'h1',
      Body: '{"id":1}',
      Attributes: { ApproximateReceiveCount: '3', MessageGroupId: 'tenant-1' },
      MessageAttributes: {
        type: { DataType: 'String', StringValue: 'OrderPlaced' },
        blob: { DataType: 'Binary', BinaryValue: bytes },
      },
    });
    assert.equal(ctx.getQueue(), QUEUE);
    assert.equal(ctx.getMessageId(), 'm1');
    assert.equal(ctx.getBody(), '{"id":1}');
    assert.equal(ctx.getReceiveCount(), 3);
    assert.equal(ctx.getGroupId(), 'tenant-1');
    assert.deepEqual(ctx.getAttributes(), { type: 'OrderPlaced', blob: bytes });
    assert.equal(ctx.getMessageAttributes().type.StringValue, 'OrderPlaced');
    assert.equal(ctx.getSystemAttributes().ApproximateReceiveCount, '3');
    assert.equal(ctx.getMessage().ReceiptHandle, 'h1');
  });

  it('falls back sensibly when SQS left fields out', () => {
    const ctx = context({});
    assert.equal(ctx.getMessageId(), '');
    assert.equal(ctx.getBody(), '');
    assert.equal(ctx.getReceiveCount(), 1);
    assert.equal(ctx.getGroupId(), undefined);
    assert.deepEqual(ctx.getAttributes(), {});
    assert.deepEqual(ctx.getMessageAttributes(), {});
    assert.deepEqual(ctx.getSystemAttributes(), {});
    assert.equal(context({ Attributes: { ApproximateReceiveCount: 'zero' } }).getReceiveCount(), 1);
    assert.equal(context({ Attributes: { ApproximateReceiveCount: '0' } }).getReceiveCount(), 1);
  });

  it('delegates delete and changeVisibility to the dispatcher', async () => {
    const calls: string[] = [];
    const ctx = context(
      {},
      {
        delete: async () => {
          calls.push('delete');
        },
        changeVisibility: async seconds => {
          calls.push(`visibility:${seconds}`);
        },
      },
    );
    await ctx.delete();
    await ctx.changeVisibility(45);
    assert.deepEqual(calls, ['delete', 'visibility:45']);
  });
});

describe('defaultSqsErrorMapper and toDeadLetterMessage', () => {
  it('retries every error by default', async () => {
    assert.equal(await defaultSqsErrorMapper(new Error('x'), context({})), 'retry');
  });

  it('copies the body byte for byte, with its attributes when it has any', () => {
    assert.deepEqual(toDeadLetterMessage(context({ Body: ' raw text ' })), { body: ' raw text ' });
    const attributes = { type: { DataType: 'String', StringValue: 'x' } };
    const message = toDeadLetterMessage(context({ Body: '{}', MessageAttributes: attributes }));
    assert.deepEqual(message, { body: '{}', attributes });
    assert.notEqual(message.attributes, attributes);
  });
});

describe('SqsClients', () => {
  it('builds the default client and the named ones through the factory', () => {
    const built: string[] = [];
    const drivers = new Map<string, FakeDriver>();
    const clients = new SqsClients({
      client: { region: 'eu-west-1' },
      clients: { us: { region: 'us-east-1' } },
      driverFactory: (input, name) => {
        built.push(`${name}:${(input as { region: string }).region}`);
        const driver = createFakeDriver({ client: { name } });
        drivers.set(name, driver);
        return driver;
      },
    });
    assert.deepEqual(built, ['default:eu-west-1', 'us:us-east-1']);
    assert.deepEqual(clients.names(), [DEFAULT_SQS_CLIENT, 'us']);
    assert.deepEqual(clients.get(), { name: 'default' });
    assert.deepEqual(clients.get('us'), { name: 'us' });
    assert.equal(clients.driver('us'), drivers.get('us'));
    assert.throws(() => clients.driver('eu'), /No SQS client named "eu" is configured \(configured: default, us\)/);

    clients.onApplicationShutdown();
    assert.equal(drivers.get('default')?.destroyed, 1);
    assert.equal(drivers.get('us')?.destroyed, 1);
  });

  it('defaults to an AWS client with the SDK defaults, and reserves "default"', () => {
    const clients = new SqsClients({ client: { region: 'us-east-1' } });
    assert.equal((clients.get() as { constructor: { name: string } }).constructor.name, 'SQSClient');
    clients.onApplicationShutdown();
    const bare = new SqsClients({ driverFactory: () => createFakeDriver() });
    assert.deepEqual(bare.names(), ['default']);
    assert.throws(
      () => new SqsClients({ clients: { default: {} }, driverFactory: () => createFakeDriver() }),
      /"clients.default" is reserved/,
    );
  });
});

describe('SqsQueueRegistry', () => {
  function registry(queues: ConstructorParameters<typeof SqsQueueRegistry>[0], driver = createFakeDriver()) {
    const clients = new SqsClients({
      clients: { us: {} },
      driverFactory: (_input, name) => (name === 'us' ? createFakeDriver({ client: 'us' }) : driver),
    });
    return new SqsQueueRegistry(queues, clients);
  }

  it('normalizes a key: configured or not, a URL or a name', () => {
    const queues = registry({
      byName: 'orders-prod',
      byUrl: 'https://sqs.eu-west-1.amazonaws.com/123456789012/orders',
      detailed: { name: 'payments', concurrency: 3 },
      broken: { concurrency: 2 },
    });
    assert.deepEqual(queues.options('byName'), { name: 'orders-prod' });
    assert.deepEqual(queues.options('byUrl'), { url: 'https://sqs.eu-west-1.amazonaws.com/123456789012/orders' });
    assert.deepEqual(queues.options('detailed'), { name: 'payments', concurrency: 3 });
    assert.deepEqual(queues.options('plain-name'), { name: 'plain-name' });
    assert.deepEqual(queues.options('HTTP://host/acct/raw'), { url: 'HTTP://host/acct/raw' });
    assert.throws(() => queues.options('broken'), /The queue "broken" needs a "name" or a "url"/);
  });

  it('resolves a URL as is, its name from the path, and FIFO from the suffix', async () => {
    const queues = registry({ fifo: { url: 'http://localhost:9324/000000000000/ledger.fifo/' } });
    const resolved = await queues.resolve('fifo');
    assert.deepEqual(
      { ...resolved, driver: undefined },
      {
        key: 'fifo',
        name: 'ledger.fifo',
        url: 'http://localhost:9324/000000000000/ledger.fifo/',
        isFifo: true,
        client: 'default',
        driver: undefined,
      },
    );
  });

  it('looks a name up once per client and account, and does not cache a failure', async () => {
    let lookups = 0;
    let fail = true;
    const driver = createFakeDriver({
      getQueueUrl: async (name, owner) => {
        lookups += 1;
        if (fail) {
          fail = false;
          throw new Error('throttled');
        }
        return `https://q/${owner ?? 'self'}/${name}`;
      },
    });
    const queues = registry({ partner: { name: 'events', ownerAccountId: '210987654321' } }, driver);

    await assert.rejects(queues.resolve('orders'), /throttled/);
    const first = await queues.resolve('orders');
    const second = await queues.resolve('orders');
    const partner = await queues.resolve('partner');

    assert.equal(first.url, 'https://q/self/orders');
    assert.equal(second.url, first.url);
    assert.equal(partner.url, 'https://q/210987654321/events');
    assert.equal(partner.name, 'events');
    assert.equal(lookups, 3);
  });

  it('routes a queue to its named client, and refuses an unknown one', async () => {
    const queues = registry({ audit: { url: 'https://q/audit', client: 'us' }, lost: { name: 'x', client: 'eu' } });
    const audit = await queues.resolve('audit');
    assert.equal(audit.client, 'us');
    assert.equal(audit.driver.client, 'us');
    await assert.rejects(queues.resolve('lost'), /No SQS client named "eu"/);
  });
});

describe('resolveConsumerOptions', () => {
  it('takes each value from the first layer that sets it, then the defaults', () => {
    assert.deepEqual(resolveConsumerOptions([{}, {}], undefined, 'A.handle'), {
      concurrency: 1,
      maxMessages: 10,
      waitTimeSeconds: 20,
      receiveTimeoutMs: 30_000,
      visibilityTimeout: undefined,
      effectiveVisibilityTimeout: undefined,
    });
    assert.deepEqual(
      resolveConsumerOptions(
        [{ concurrency: 8 }, { concurrency: 2, maxMessages: 5, waitTimeSeconds: 5 }, { visibilityTimeout: 60 }],
        30,
        'A.handle',
      ),
      {
        concurrency: 8,
        maxMessages: 5,
        waitTimeSeconds: 5,
        receiveTimeoutMs: 15_000,
        visibilityTimeout: 60,
        effectiveVisibilityTimeout: 60,
      },
    );
    assert.equal(resolveConsumerOptions([{}], 45, 'A.handle').effectiveVisibilityTimeout, 45);
    assert.equal(resolveConsumerOptions([{ receiveTimeoutMs: 25_000 }], 45, 'A.handle').receiveTimeoutMs, 25_000);
  });

  it('refuses settings SQS would refuse, naming the handler and every problem', () => {
    assert.throws(
      () =>
        resolveConsumerOptions(
          [{ concurrency: 0, maxMessages: 11, waitTimeSeconds: 0, receiveTimeoutMs: 0, visibilityTimeout: 43_201 }],
          undefined,
          'Orders.placed',
        ),
      (error: Error) =>
        error.message.startsWith('Invalid consumer options for Orders.placed: concurrency must be') &&
        error.message.includes('maxMessages must be') &&
        error.message.includes('short polling is not supported') &&
        error.message.includes('receiveTimeoutMs must exceed') &&
        error.message.includes('visibilityTimeout must be'),
    );
    for (const options of [{ concurrency: 1.5 }, { maxMessages: 0 }, { waitTimeSeconds: 21 }, { visibilityTimeout: -1 }]) {
      assert.throws(() => resolveConsumerOptions([options], undefined, 'A.b'), /Invalid consumer options/);
    }
  });
});
