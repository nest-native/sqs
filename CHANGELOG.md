# Changelog

All notable user-facing changes to `@nest-native/sqs` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

- **The module.** `SqsModule.forRoot()`, `forRootAsync()` and `forFeature()`,
  with logical queues (a name resolved with `GetQueueUrl` and cached, a URL, a
  cross-account name) and named clients for other regions or accounts. The
  package drives `@aws-sdk/client-sqs` directly through a driver seam and keeps
  `"dependencies": {}`.
- **Consumers.** `@SqsHandler` on any provider or controller (with an optional
  `@SqsConsumer` default queue) runs each message through the full Nest
  enhancer pipeline, with `@SqsMessage()`, `@SqsAttributes()`, `@SqsCtx()` and
  `@SqsRawMessage()`. A receive never asks for more messages than the consumer
  has free slots (`concurrency`, default 1), and every long poll runs under a
  deadline; a failed receive backs off from 1 s to 30 s.
- **Acknowledgement.** Returning deletes the message; an error goes to the
  `errorMapper` (`'retry'` by default, or `'delete'`). A retry makes the message
  visible again after a backoff that grows with `ApproximateReceiveCount`, never
  below one second and within the 12-hour budget. `ctx.delete()` and
  `ctx.changeVisibility()` give manual control.
- **Bootstrap check.** One `GetQueueAttributes` per consumed queue reads
  `VisibilityTimeout` and `RedrivePolicy`: a missing queue fails the bootstrap,
  a queue without a dead-letter queue is warned about, and a denied read falls
  back to the consumer's `visibilityTimeout` option.
- **Graceful shutdown** in `beforeApplicationShutdown`: stop receiving (waiting
  for the long poll and releasing what it returned), drain for up to
  `drainTimeoutMs`, release what is still running, flush every pending delete.
- **Producer.** `SqsProducerService.send()` and `sendBatch()` (split into
  requests of at most 10 messages and 1 MiB, with per-message results), with
  client-side validation of every rule SQS enforces.
- **Dead letters.** The redrive policy is the dead-letter mechanism;
  `toDeadLetterMessage()` and an exception filter make the documented
  "dead-letter now" recipe.
- **Testing.** `@nest-native/sqs/testing`: `InMemorySqs` (SQS's visibility,
  receipt-handle, receive-count and redrive semantics, strict about every
  argument), `VirtualClock` and `SqsTestModule`. One semantics contract runs
  against it and against a real endpoint (ElasticMQ in CI).
