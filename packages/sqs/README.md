# @nest-native/sqs

Decorator-first NestJS integration for Amazon SQS, built directly on
`@aws-sdk/client-sqs`.

> **Status: `0.1.0` (`0.x`)**, in development and not yet published. FIFO
> ordering, batch handlers, the visibility heartbeat, the health indicator and
> the `@nestjs/outbox` transport are the next milestone.

```bash
npm i @nest-native/sqs @aws-sdk/client-sqs
```

## The module

```ts
SqsModule.forRoot({
  client: { region: 'eu-west-1' },                  // an SQSClient configuration, or an SQSClient
  clients: { us: { region: 'us-east-1' } },          // named clients: other regions or accounts
  queues: {
    orders: 'orders-prod',                           // a name, resolved with GetQueueUrl (cached)
    audit: { name: 'audit-events', client: 'us' },   // a name on a named client
    partner: { name: 'events', ownerAccountId: '210987654321' }, // cross-account by name
    billing: 'https://sqs.eu-west-1.amazonaws.com/123456789012/billing', // a URL, used as is
  },
  consumerDefaults: { concurrency: 4 },
});
```

`SqsModule.forRootAsync({ imports, inject, useFactory, extraProviders })`
builds the same options from injected configuration, so queue names come from
`ConfigService` and never from `process.env` read while a decorator runs.
`forRoot` and `forRootAsync` are global unless `isGlobal: false`.
`SqsModule.forFeature([Handlers])` registers handler classes in a feature module.

Handlers and the producer name queues by key. A key that is not configured is
taken as a queue name on the default client, so `@SqsHandler('orders')` works
with no `queues` entry at all.

### Emulators

Point the client at the emulator and use queue names:

```ts
SqsModule.forRoot({
  client: {
    endpoint: 'http://localhost:9324',
    region: 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  },
});
```

With `endpoint` set, the SDK sends every request there, and its "QueueUrl
differs from SQSClient resolved endpoint" warning does not appear. The SDK has
spoken the AWS JSON protocol since `@aws-sdk/client-sqs` 3.446.0, so the
emulator must too: ElasticMQ from 1.4.0, or LocalStack.

## Consuming

```ts
@Injectable()
export class OrdersConsumer {
  constructor(private readonly orders: OrdersService) {}

  @SqsHandler('orders')
  @UseInterceptors(TracingInterceptor)
  async placed(@SqsMessage(new ValidationPipe()) order: OrderDto, @SqsCtx() ctx: SqsContext) {
    await this.orders.apply(order, { signal: ctx.signal });
  }
}
```

- Handlers are discovered on any provider or controller. `@SqsConsumer(queue)`
  on the class sets a default queue for its handlers.
- A queue has exactly one handler: SQS delivers each message to one receiver.
  For more throughput, raise `concurrency`.
- Every message runs through guards, interceptors, pipes and exception filters,
  global ones included, under the `'rpc'` execution-context type.
  Request-scoped handlers get a fresh instance per message, and the method is
  looked up on the instance at dispatch time, so wrappers applied after startup
  take effect.
- Consumers start in `onApplicationBootstrap`. Before they do, each queue is
  resolved and read with one `GetQueueAttributes` (`VisibilityTimeout`,
  `RedrivePolicy`): a missing queue fails the bootstrap, naming the handler, and
  a queue without a `RedrivePolicy` gets a warning.

### Parameters

| Decorator | Injects |
| --- | --- |
| `@SqsMessage()` | the body parsed as JSON, or the body text when it is not JSON |
| `@SqsMessage('id')` | one property of an object payload |
| `@SqsAttributes()` / `@SqsAttributes('type')` | the message attributes, decoded, or one of them |
| `@SqsCtx()` | the `SqsContext`: queue, raw message, receive count, `signal`, `delete()`, `changeVisibility()` |
| `@SqsRawMessage()` | the message exactly as SQS returned it (the SDK's `Message`) |

A handler without parameter decorators receives `(payload, context)`.

### Consumer options

| Option | Default | Meaning |
| --- | --- | --- |
| `concurrency` | `1` | messages held at once, from receive to settle |
| `maxMessages` | `10` | `MaxNumberOfMessages` per receive (1–10), capped by free slots |
| `waitTimeSeconds` | `20` | the long poll (1–20; short polling is not supported) |
| `receiveTimeoutMs` | `waitTimeSeconds × 1000 + 10 000` | the deadline of a receive; it is aborted and retried after it |
| `visibilityTimeout` | the queue's | a per-receive `VisibilityTimeout`, in seconds |

Set them per queue (`queues.<key>`), on `@SqsHandler(queue, options)`, or in
`consumerDefaults`; the queue's configuration wins, then the decorator, then the
defaults.

**The default concurrency is 1**, as `@ssut/nestjs-sqs`'s `batchSize: 1` was:
one message at a time, so one slow message holds the consumer. Raise it when
your handler can run in parallel.

### Errors and retries

Returning deletes the message. An error that no exception filter handled goes
to the error mapper:

```ts
SqsModule.forRoot({
  errorMapper: (error, ctx) => (error instanceof UnknownEventError ? 'delete' : 'retry'),
  retryBackoff: { initialDelaySeconds: 1, maxDelaySeconds: 300, multiplier: 2, jitter: 'equal' },
});
```

- `'retry'` (the default for every error) makes the message visible again after
  a delay that grows with its `ApproximateReceiveCount`: `initialDelaySeconds ×
  multiplier^(count − 1)`, capped, jittered, never below one second, and never
  past the 12 hours SQS allows from the receive. SQS keeps the count, so the
  backoff survives restarts. `retryBackoff: false` leaves the visibility alone:
  the message returns when its visibility timeout expires.
- `'delete'` drops the message.
- A mapper that throws, or answers anything else, retries.

A message that keeps failing reaches the queue's **dead-letter queue** through
its redrive policy, after `maxReceiveCount` receives: SQS moves it atomically,
and DLQ redrive can return it to its source. Configure one for every consumed
queue.

#### Dead-letter now

When a failure is certain, an exception filter can move the message at once:

```ts
@Catch(PoisonMessageError)
@Injectable()
export class DeadLetterFilter implements ExceptionFilter {
  constructor(private readonly producer: SqsProducerService) {}

  async catch(_error: PoisonMessageError, host: ArgumentsHost): Promise<void> {
    const ctx = host.switchToRpc().getContext<SqsContext>();
    await this.producer.send('orders-dlq', toDeadLetterMessage(ctx));
  }
}
```

The filter is awaited before the message is settled, and it handled the error,
so the message is deleted after the copy is sent. `toDeadLetterMessage` keeps
the body byte for byte and the message attributes. A copy sent this way records
no source queue: to redrive it, name the destination.

#### Manual control

`ctx.delete()` deletes the message now (the transport then does not delete it
again, and an error thrown afterwards cannot retry it). `ctx.changeVisibility(s)`
changes its visibility, counted from now.

### Shutdown

Call `app.enableShutdownHooks()`. In `beforeApplicationShutdown`, each consumer
stops receiving — waiting for the long poll in flight, at most its deadline,
and releasing what it returned with visibility 0 — then drains the messages in
flight, for at most `shutdown.drainTimeoutMs` (20 s), releases whatever is still
running (its `ctx.signal` aborts), and sends every pending delete. NestJS
finishes this phase in every module before any `onApplicationShutdown` runs, so
release the resources your handlers use there, not in `onModuleDestroy`.
`shutdown.abortReceive: true` aborts the long poll instead of waiting for it.

## Producing

```ts
await producer.send('orders', { body: order, attributes: { type: 'OrderPlaced', version: 2 } });
await producer.send('ledger.fifo', { body: entry, groupId: entry.accountId, deduplicationId: entry.id });
const results = await producer.sendBatch('orders', orders.map(body => ({ body })));
```

- A string body is sent as is; anything else as JSON. Bytes are refused (encode
  them first).
- Every message is checked before it is sent — the body's characters, the size
  (body plus attributes, 1 MiB by default, `producer.maxMessageBytes`), at most
  10 attributes, valid attribute names, `groupId` on FIFO queues,
  `deduplicationId` on FIFO only, `delaySeconds` 0–900 on standard queues only —
  and refused with a `SqsMessageValidationError` that names the rule.
- `groupId` on a standard queue opts the message into fair queueing; the
  producer never sets one for you.
- `sendBatch()` splits into requests of at most 10 messages and 1 MiB, sent one
  at a time, and resolves with every result in order. When not every message was
  accepted it rejects with a `SqsBatchSendError` listing each message by index:
  `successful`, `failed` (refused), `uncertain` (its request failed) and
  `notAttempted`. On a FIFO queue the batch stops at the first refusal, so no
  message overtakes an earlier one.
- `send()` rejects with a `SqsSendError` carrying SQS's code when SQS refuses
  the message.

`@InjectSqsClient()` injects the default `SQSClient` (`SqsClients.get(name)` the
named ones) for anything else: `PurgeQueue`, queue attributes,
`StartMessageMoveTask`.

## Testing

```ts
const moduleRef = await Test.createTestingModule({
  imports: [SqsTestModule.forRoot({ queues: { orders: 'orders' } })],
  providers: [OrdersConsumer, OrdersService],
}).compile();
await moduleRef.init();

const sqs = moduleRef.get(InMemorySqs);
sqs.sendMessage('orders', JSON.stringify({ id: 1 }));
await sqs.idle();                          // every polled queue: nothing visible, nothing in flight
await (sqs.clock as VirtualClock).advance(2_000); // let a retry's visibility expire
```

`InMemorySqs` keeps SQS's semantics — visibility timeouts, a new receipt handle
per receive (an older one deletes nothing), receive counts, redrive to a
dead-letter queue, long polling, the 12-hour budget — and refuses every argument
SQS refuses, with SQS's error code. It runs on a `VirtualClock`, so a test moves
time instead of sleeping. FIFO queues are not modeled yet.

## IAM

- Consumer: `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
  `sqs:ChangeMessageVisibility`, `sqs:GetQueueUrl` (queue names), and
  `sqs:GetQueueAttributes` for the bootstrap read (optional: without it, the
  consumer warns and uses its `visibilityTimeout` option).
- Producer: `sqs:SendMessage`, `sqs:GetQueueUrl`.
- Queues encrypted with a customer key: `kms:Decrypt`, and
  `kms:GenerateDataKey` for producers.

The batch operations need no permission of their own.

## Migrating from @ssut/nestjs-sqs

| `@ssut/nestjs-sqs` 3.0.1 | `@nest-native/sqs` |
| --- | --- |
| `SqsModule.register({ consumers, producers })` | `SqsModule.forRoot({ client, queues })` |
| `registerAsync({ useFactory, inject, imports })` | `forRootAsync({ useFactory, inject, imports, extraProviders })` |
| consumer `{ name, queueUrl }` / producer `{ name, queueUrl }` | `queues: { [name]: { url } }`, shared by both sides; producers need no registration |
| `region`, `sqs: new SQSClient()` | `client: { region }` or `client: existingClient` |
| `batchSize` | `concurrency` (and `maxMessages`); the default stays 1 |
| `waitTimeSeconds`, `visibilityTimeout` | the same names |
| `terminateVisibilityTimeout` | `retryBackoff` |
| `messageAttributeNames`, `attributeNames` | every attribute is always requested |
| `pollingCompleteWaitTimeMs`, `stopOptions` | `shutdown.drainTimeoutMs`, `shutdown.abortReceive` |
| `@SqsMessageHandler(name)` | `@SqsHandler(name)` |
| `handle(message: Message)` | `handle(@SqsRawMessage() message: Message)` |
| a handler that returns deletes, `undefined` included | the same |
| `@SqsConsumerEventHandler(name, 'processing_error')` | the error mapper, an exception filter, or a global filter; failures are logged through Nest's `Logger` |
| `@SqsConsumerEventHandler(name, 'error')` | receive and settle failures are logged through Nest's `Logger` |
| `SqsService.send(name, message)` | `SqsProducerService.send(queue, message)`; no `id` needed |
| `SqsService.purgeQueue` / `getQueueAttributes` | `@InjectSqsClient()` and the SDK command |
| `options.logger` | `app.useLogger()` |

## License

MIT © 2026 Rodrigo Nogueira.
