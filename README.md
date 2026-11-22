# @nest-native/sqs

<p align="center">Decorator-first NestJS integration for Amazon SQS, built directly on @aws-sdk/client-sqs.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/sqs"><img src="https://img.shields.io/npm/v/@nest-native/sqs.svg" alt="NPM Version" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
</p>

> [!NOTE]
> **Status: `0.1.0` (`0.x`)**, in development and not yet published. Built so
> far: the module (`SqsModule.forRoot()` / `forRootAsync()` / `forFeature()`),
> logical queues and named clients, `@SqsHandler` with the full Nest enhancer
> pipeline, the parameter decorators, error mapping with a receive-count retry
> backoff, FIFO queues with per-group ordering, the visibility heartbeat,
> graceful shutdown, `SqsProducerService`, and the testing utilities
> (`InMemorySqs`, FIFO queues included, `VirtualClock`, `SqsTestModule`). Batch
> handlers, the handler timeout, the health indicator and the `@nestjs/outbox`
> transport follow. The published package keeps `"dependencies": {}`.

## What This Is

`@nest-native/sqs` consumes and produces Amazon SQS messages the Nest way: a
`@SqsHandler` method per queue, constructor injection, and the full enhancer
pipeline — guards, interceptors, pipes and exception filters — around every
message. It drives `@aws-sdk/client-sqs` directly, through a small driver seam,
with no `sqs-consumer` or `sqs-producer` layer in between, and it keeps SQS's
own semantics in view: visibility timeouts, receipt handles, receive counts and
redrive policies are part of the API.

It is meant as the maintained successor of `@ssut/nestjs-sqs`, with a migration
path from it.

## What It Does Differently

- **The SDK, driven directly.** No wrapper layer is pinned between your
  application and `@aws-sdk/client-sqs`, so there is one SDK in the tree and no
  wrapper major to wait for.
- **Nest all the way down.** Handlers are discovered on any provider or
  controller, resolve their dependencies (request-scoped ones per message), and
  run guards, interceptors, pipes and filters, global ones included.
- **Delete after success, retry with a backoff.** A message is deleted when its
  handler returns. A failure is retried after a delay that grows with
  `ApproximateReceiveCount` — kept by SQS, so it survives restarts — and never
  below one second; the queue's redrive policy moves a message that keeps
  failing to its dead-letter queue.
- **FIFO order under concurrency.** On a FIFO queue each message group runs
  one message at a time, in order, while different groups run side by side; a
  retry holds its group, so no later message overtakes the failed one.
- **No message reappears mid-flight.** A heartbeat extends the visibility of
  every message the consumer holds — running, or waiting behind its FIFO group
  — within the 12 hours SQS allows from a receive.
- **No silent stalls.** Every long poll runs under a deadline (the SDK sets no
  request timeout of its own), failed receives back off and retry, and a receive
  never asks for more messages than the consumer has room for.
- **Graceful shutdown.** Stop receiving, release what the last poll returned
  and what waits behind a FIFO group, drain what is running, release what
  outlives the drain — before any `onApplicationShutdown` closes your database
  pools.
- **Tests without AWS.** `InMemorySqs` has SQS's visibility semantics, refuses
  what SQS refuses, and runs on a virtual clock; the same contract spec runs
  against it and against a real endpoint.

## Compatibility

| Runtime | Supported line |
| --- | --- |
| Node.js | `>=22` (`>=22.12` with NestJS 12) |
| NestJS | `^11.0.0 \|\| ^12.0.0` |
| `@aws-sdk/client-sqs` | `^3.600.0` |
| Emulators | any that speaks the AWS JSON protocol: ElasticMQ `>=1.4.0` (tested), LocalStack |

## Installation

```bash
npm i @nest-native/sqs @aws-sdk/client-sqs
```

## Usage

```ts
import { Injectable, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { SqsHandler, SqsMessage, SqsModule, SqsProducerService } from '@nest-native/sqs';

@Module({
  imports: [
    ConfigModule.forRoot(),
    SqsModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        client: { region: config.getOrThrow('AWS_REGION') },
        queues: { orders: { name: config.getOrThrow('ORDERS_QUEUE'), concurrency: 5 } },
      }),
    }),
  ],
  providers: [OrdersConsumer, OrdersService],
})
export class AppModule {}

@Injectable()
export class OrdersConsumer {
  @SqsHandler('orders')
  async placed(@SqsMessage() order: OrderPlaced): Promise<void> {
    // returning deletes the message; throwing retries it after a backoff
  }
}

@Injectable()
export class OrdersService {
  constructor(private readonly sqs: SqsProducerService) {}

  place(order: OrderPlaced) {
    return this.sqs.send('orders', { body: order, attributes: { type: 'OrderPlaced' } });
  }
}
```

Call `app.enableShutdownHooks()` so a `SIGTERM` drains the consumers.

> [!IMPORTANT]
> **A consumer handles one message at a time by default** (`concurrency: 1`),
> as `@ssut/nestjs-sqs` did with its default `batchSize: 1`. That keeps a
> migration from multiplying the load on what your handlers call, at the cost
> of throughput: one slow message holds the consumer. Raise `concurrency` per
> queue (or in `consumerDefaults`) when your handlers can run in parallel; a
> receive then asks for up to `min(10, free slots)` messages.

The package README covers the options, the error mapper and the dead-letter
recipe, the producer, emulators, and testing:
[packages/sqs/README.md](packages/sqs/README.md).

## Repository Layout

- [`packages/sqs`](packages/sqs): the `@nest-native/sqs` package
- [`scripts`](scripts): coverage, complexity, release-check and integration helpers
- [`GUIDELINES_NEST_SQS.md`](GUIDELINES_NEST_SQS.md): the project constitution
- [`CHANGELOG.md`](CHANGELOG.md), [`SECURITY.md`](SECURITY.md)

## Quality Gates

- build and typecheck on Node.js 22 and 24; tests on Node.js 22
- coverage with `c8`, enforced at 100% for statements, branches, functions and lines
- cognitive complexity enforced with Biome, threshold `15`
- a NestJS matrix that installs each end of the peer range (`11.0.0` and `^12`),
  and an SDK leg at the `@aws-sdk/client-sqs` floor (`3.600.0`)
- package tarball validation, README link and version checks, and an audit of
  the published supply chain
- a real-backend job against ElasticMQ that runs the semantics contract and a
  Nest application end to end, through a runner that fails if anything was
  skipped

Run the local gate with `npm run ci`. With Docker, `npm run infra:up && npm run
test:full` adds the gated suite against a local ElasticMQ.

## License

[MIT](LICENSE) © 2026 Rodrigo Nogueira.

Part of the [nest-native](https://nest-native.dev/) family of NestJS integrations.
