# GUIDELINES_NEST_SQS.md

## Core Philosophy — This library MUST feel native in NestJS projects

Every decision follows NestJS philosophy, as `@nestjs/microservices` does for its
transports, while keeping SQS's own semantics visible: visibility timeouts,
receipt handles, receive counts and redrive policies are part of the API, not
details hidden behind it. The bar: feel like a first-class Nest transport, drive
`@aws-sdk/client-sqs` directly, never hide SQS semantics.

### 1. Overall Architecture Assumptions (never break these)

- First-class NestJS integration, not a thin wrapper around the SDK, and not a
  wrapper around `sqs-consumer` or `sqs-producer`: the package drives
  `@aws-sdk/client-sqs` through its own driver seam (`driver.ts`).
- Decorator-first, OOP, heavy use of NestJS DI.
- Current stabilization support line:
  - Node.js `>=22` (`>=22.12` on the NestJS 12 end: 12 is ESM-only and
    `require(esm)` is behind a flag before 22.12.0; `engines` stays `>=22`)
  - NestJS `^11.0.0 || ^12.0.0`
  - `@aws-sdk/client-sqs` `^3.600.0` — the floor of `@ssut/nestjs-sqs` 3.0.1,
    so migrating needs no SDK upgrade
- **Peer majors are widened, never swapped.** A new peer major widens the
  published range; the devDependency and the lockfile stay on the older major,
  and a dedicated CI leg installs the newer one with `--no-save`. Both ends of
  every range are tested claims.
- The `@aws-sdk/client-sqs` range is tested at both ends: the lockfile version,
  and the exact floor (`3.600.0`) in its own CI leg.
- Full integration with the NestJS enhancer pipeline is NON-NEGOTIABLE:
  `@UseGuards`, `@UseInterceptors`, `@UsePipes` and `@UseFilters` work on
  handler methods, global enhancers apply, and request-scoped providers are
  resolved per message.
- The package is transport-only. It imposes no HTTP adapter.
- Validation is app-owned: class-validator DTOs through `ValidationPipe`, or
  Zod. Both are optional peers.

### 2. Public API Assumptions (this is what users will copy-paste)

- Module: `SqsModule.forRoot(options)`, `SqsModule.forRootAsync(options)`,
  `SqsModule.forFeature([HandlerClass])`.
- Queues are logical keys, configured under `queues` (a name, a URL, an owner
  account, a named client). A key that is not configured is a queue name.
- Clients: `client` (the default) and `clients` (named, for other regions or
  accounts); each an `SQSClient` configuration or an existing client.
- Decorators: `@SqsHandler(queue?, options?)` on a method, `@SqsConsumer(queue?)`
  optional on a class; parameters `@SqsMessage()`, `@SqsAttributes()`,
  `@SqsCtx()`, `@SqsRawMessage()`.
- Producer: `SqsProducerService` with `send`, `sendBatch` and `resolveQueue`;
  `@InjectSqsClient()` and `SqsClients.get(name)` for the raw clients.
- Errors: `errorMapper` returns `'retry'` or `'delete'`; `retryBackoff` spaces
  retries out; `toDeadLetterMessage(context)` is the dead-letter primitive.
- Testing: `@nest-native/sqs/testing` exports `InMemorySqs`, `VirtualClock` and
  `SqsTestModule`.
- A migration path from `@ssut/nestjs-sqs` must exist and stay current.

### 3. First-Version Scope Discipline

- Milestone 1 (built): the module, logical queues and named clients, the driver
  seam, `@SqsHandler` and the parameter decorators through the full enhancer
  pipeline, the poller with capacity-gated receives and a deadline,
  return-deletes, the error mapper and the receive-count backoff, the bootstrap
  attribute read, the producer, `/testing`, the semantics contract, and graceful
  shutdown.
- Milestone 2 (built so far): FIFO per-group ordering, with retry-then-release
  and FIFO queues in `InMemorySqs` (group locking, the deduplication window,
  sequence numbers), and the visibility heartbeat.
- Milestone 2 (planned): batch handlers with partial failures, the handler
  timeout, `SqsConsumerRegistry`, the health indicator, the `/outbox` transport
  for `@nestjs/outbox`, the samples and the documentation site.
- v0.1 does NOT ship: an explicit dead-letter move (the redrive policy is the
  dead-letter mechanism; the recipe below is the documented pattern), the SQS
  Extended Client (S3 payloads), automatic coalescing of `send()` calls, queue
  provisioning, a Lambda adapter, SNS envelope unwrapping, several handlers per
  queue, request-reply over SQS, a `@nestjs/microservices` transport or
  `ClientProxy` adapter, client-side encryption, ARN queue references,
  transport-level event hooks (`@OnSqsEvent`), and a real-AWS CI leg.

### 4. Sample Folder Rules

- `sample/00-showcase` demonstrates the producer and consumers wired together,
  feature modules, constructor DI and request-scoped providers, the enhancers,
  the error mapper and the dead-letter recipe, and graceful shutdown.
- Focused samples under `sample/01-*` … isolate one topic each.
- Never simplify the showcase for brevity.

### 5. Implementation Invariants

Each is enforced by a test; a change to one updates its test in the same PR.

1. A message is deleted only after its handler and enhancers completed, the
   error mapper chose `'delete'`, or the handler called `ctx.delete()`.
2. A message the transport never handed to a handler is released (visibility 0),
   never deleted.
3. No retry sets a visibility below one second. There is no `WaitTimeSeconds 0`.
   Every receive runs under a deadline (`receiveTimeoutMs`), because the SDK
   sets no request timeout of its own.
4. A receive never asks for more messages than the consumer has free permits;
   a permit is held from receive to settle, by a FIFO message waiting behind
   an earlier one of its group as much as by a running one.
5. A retry delay grows with `ApproximateReceiveCount` and never passes the
   12 hours SQS allows from the receive.
6. On a FIFO queue, the held messages of one group run one at a time, in
   receive order: a message starts only after the one before it finished and
   SQS answered its delete. Different groups run concurrently. A `'retry'`
   first sets the failed message's visibility (the backoff; nothing with
   `retryBackoff: false`), and only once SQS answered does it release the
   group's later held messages (visibility 0, batched): none of them may run
   before SQS redelivers the failed message. A delete SQS refuses, automatic
   or `ctx.delete()`, releases them the same way, because the message is still
   in the queue.
7. The heartbeat extends every held message — running, or waiting behind its
   group — from receive to settle, and never past the 12 hours SQS allows from
   the receive. A message's heartbeat stops, and the extension it has in
   flight is answered, before the message is deleted, retried or released, so
   no extension overtakes a settle; `ctx.changeVisibility()` stops it too.
8. Every batch response is checked entry by entry: a batch call returns HTTP
   200 even when some of its entries failed.
9. Shutdown order: stop receiving, release what the last receive returned,
   release the held messages that have not started (FIFO messages waiting
   behind their group), drain (bounded by `drainTimeoutMs`), release what is
   still running, flush every pending settle — all inside
   `beforeApplicationShutdown`.
10. The in-memory SQS refuses every argument SQS refuses, with SQS's error code.
    A new driver argument gets a case in the semantics contract, which runs
    against both `InMemorySqs` and the real endpoint, in the same PR.
11. The published package keeps `"dependencies": {}`.

### 6. Non-Negotiable Style & Patterns

- NestJS naming conventions, constructor injection.
- Global, module and method-level enhancers always work.
- Documentation follows Nest-style clarity without claiming official Nest or
  AWS status, and never positions against other packages: cite neutral sources.
- Onboarding stays on `SqsModule`, `@SqsHandler` and `SqsProducerService`;
  advanced features stay in their own sections.

### 7. When In Doubt

- Ask: "Would this feel natural to a Nest developer, and does it leave SQS's
  semantics visible?" If either answer is no, redesign.

### 8. Differentiation Strategy

- The SDK, driven directly: no pinned wrapper layer between the application and
  `@aws-sdk/client-sqs`.
- The full enhancer pipeline and correct DI on every handler.
- Visible, tested SQS semantics: receive-count backoff, the 12-hour budget,
  per-entry batch results, a receive deadline, graceful release on shutdown.
- An in-memory SQS with real visibility semantics, held to the same contract as
  a real endpoint.

### 9. Security Review Requirements (MANDATORY)

- Every PR includes an explicit security pass.
- Supply chain: every dependency change is reviewed for legitimacy and
  lifecycle scripts; `packages/sqs/package.json` keeps `"dependencies": {}`;
  runtime requirements are peers, build tools are devDependencies; no unpinned
  Git or URL dependencies.
- Application security: authorization in handlers, input validation of
  payloads, no credentials, account IDs or queue URLs of real systems in
  samples, logs or docs, and least-privilege IAM documented:
  - consumer: `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
    `sqs:ChangeMessageVisibility`, `sqs:GetQueueUrl` for names, and optionally
    `sqs:GetQueueAttributes` (the bootstrap read);
  - producer: `sqs:SendMessage`, `sqs:GetQueueUrl`;
  - SSE-KMS queues: `kms:Decrypt`, and `kms:GenerateDataKey` for producers.
  The batch operations are authorized by the single-action permissions.
- Audit scope: `security:audit` audits the published surface
  (`npm audit --omit=dev --audit-level=high` on the packed tarball).
- Strictness scope: 100% coverage, complexity ≤ 15 and zero runtime
  dependencies govern `packages/sqs`; samples, docs and tooling use lighter
  rules.

### 10. Release Version Synchronization (MANDATORY)

- Version drift between `packages/sqs` and `sample/*` is a release blocker.
- Prose version literals (the `Status:` lines, badges) state the published
  version; `release:check:readme-version` enforces it, and version checks
  enumerate `packages/*` instead of hardcoding names.
- Badges are generated (`img.shields.io/npm/v/...`), never hand-written.

### 11. Cognitive Complexity Review

- CI enforces a cognitive-complexity threshold of `15` with Biome
  (`complexity/noExcessiveCognitiveComplexity`) on every source function.
- Never reduce complexity by weakening architecture, API clarity or coverage.

### 12. Accumulated Project Decisions

**The SDK is driven directly.** `@ssut/nestjs-sqs` hard-depended on
`sqs-consumer ^11` and `sqs-producer ^5`, and could not follow them:
`sqs-consumer` shipped ten majors between 2022-12 and 2026-05, both bbc packages
declare `@aws-sdk/client-sqs` as a dependency as well as a peer (two SDK copies
in one tree), and `sqs-consumer` 14.0.0 (2025-09-15) changed what a handler
returning `undefined` means — before it, the message was deleted; from it, the
message stays. A wrapper whose users return `void` cannot take that upgrade
without breaking them. The operations this package needs are six SDK commands
on a stable API (2012-11-05), so it owns the loop instead of pinning one.

**Returning deletes; throwing goes to the error mapper.** Matching
`@ssut/nestjs-sqs` 3.0.1, which deleted on any return, `undefined` included. A
thrown error that no exception filter handled reaches `errorMapper`; its default
retries every error, unlike `@nest-native/kafka`'s commit of 4xx errors: SQS
has no partition to unblock, so a failing standard message blocks nobody, and
the redrive policy keeps the message instead of dropping it.

**Dead letters are the queue's redrive policy; dead-letter-now is a recipe.**
SQS moves a message that keeps failing atomically, server-side, even when a
consumer crashes, and `StartMessageMoveTask` returns redriven messages to their
source. An explicit send-then-delete is two calls (a crash between them
duplicates), records no source queue, and cannot add metadata to a message that
already has ten attributes. So the package never moves messages itself. When a
failure is certain, the documented pattern is an exception filter that sends
`toDeadLetterMessage(context)` with `SqsProducerService` and returns: the filter
handled the error, so the message is deleted. The filter is DI-enabled and
awaited before the delete; `test/sqs-dead-letter.recipe.spec.ts` holds it.
Bootstrap warns about a consumed queue that has no `RedrivePolicy`.

**Retries back off through the visibility, from the receive count.** A
`'retry'` sets the message's visibility to `initialDelaySeconds ×
multiplier^(receiveCount − 1)`, capped at `maxDelaySeconds`, with equal jitter
by default — the shape of `@nestjs/outbox`'s relay — and never below one second.
`ApproximateReceiveCount` lives in SQS, so the backoff survives restarts and
spans instances with no state in memory. The delay is clamped to the 12-hour
budget from the receive: SQS refuses a visibility beyond it rather than
shortening it.

**The default concurrency is 1.** It matches `@ssut/nestjs-sqs`, whose
effective default (`batchSize: 1`) handled one message at a time, so a
migration does not multiply the load on a handler's dependencies. The README
says so up front, with the throughput trade-off and how to raise it.

**Every receive has a deadline.** The SDK sets no request timeout
(`DEFAULT_REQUEST_TIMEOUT = 0` in `@smithy/node-http-handler`) and its socket
timeout is opt-in, so a long poll on a half-open connection could stall the
loop. `receiveTimeoutMs` (default `waitTimeSeconds × 1000 + 10 000`) aborts it;
the loop backs off (1 s doubling to 30 s) and goes on. A receive never asks for
more messages than the consumer has free permits, so no message waits in memory
while its visibility timeout runs.

**Bootstrap reads the queue once.** One `GetQueueAttributes` per consumed queue
reads `VisibilityTimeout` and `RedrivePolicy` together. A queue that does not
exist fails the bootstrap, naming the handler. Any other failure — a missing
`sqs:GetQueueAttributes` permission, typically — is warned about once, and the
consumer falls back to its own `visibilityTimeout` option, which it then passes
on every receive; without that option the visibility is unknown, and the
heartbeat stays off, with one warning.

**Shutdown drains in `beforeApplicationShutdown`.** NestJS completes each
shutdown phase in every module before it starts the next, so a drain there
finishes before any `onApplicationShutdown` — where database modules close
their pools — whatever the order of providers within a phase, which differs
between NestJS 11 and 12 and is not a contract. `@nest-native/kafka` drains in
`onApplicationShutdown`; this package deliberately does not. The test that
holds it runs on both majors in the `nestjs-compat` legs.

**Shutdown waits for the long poll by default.** Aborting the receive in flight
would leave whatever SQS had already handed to it invisible until its visibility
timeout expired. So stop waits for it (at most its deadline) and releases what
it returned with visibility 0. `shutdown.abortReceive` trades that for speed;
`SqsTestModule` sets it, so closing a test application never waits on the
virtual clock.

**FIFO: one message per group at a time, and a retry holds its group.** A
receive returns as many messages of one group as it can, so a FIFO consumer
holds a group's messages in a chain: the first runs, the rest wait, each
holding its permit and its heartbeat, and the next runs once its predecessor
settled. Different groups run side by side, up to `concurrency`. On a `'retry'`
the failed message's visibility is set first and the rest of its chain is
released after: SQS keeps a group locked while an earlier message of it is in
flight, so the released messages wait behind the failed one and it is
redelivered first. Releasing them before the backoff is set, or letting the next
one run, would let a later message overtake it. Holding waiting messages as
permits bounds memory and the heartbeat's work, at the cost of throughput when
one group dominates a receive; `concurrency` is the knob.

**The heartbeat covers every held message.** It is on by default, extending
each held message by `extendBySeconds` (the visibility timeout) every
`intervalSeconds` (half of it) from the receive, so a handler shorter than the
interval costs no call. Waiting chain members are extended too: otherwise one
could expire while its predecessors run and reach another consumer out of
order. Each extension is clamped to the 12 hours SQS allows from the receive,
because SQS refuses a longer visibility instead of shortening it; the extension
that reaches the limit is the last, with a warning. A message's heartbeat is
stopped, and its extension in flight awaited, before any settle, so an
extension never lands after the delete or the retry visibility. The timers run
on the `Clock` seam, so tests move virtual time instead of sleeping.

**The producer never sets a message group on its own.** A `MessageGroupId` on a
standard queue opts the message into fair queueing; the producer sends one only
when the caller sets `groupId`. FIFO queues require one, and the producer refuses
a FIFO message without it before sending.

**The in-memory SQS is held to the real one.** `InMemorySqs` refuses every
argument SQS refuses and deletes nothing with an older receipt handle (the worst
case AWS documents). The semantics contract (`test/contract`) runs the same
assertions against it and against the gated endpoint, so the double cannot
drift into accepting what SQS rejects — the failure `@nest-native/kafka`'s §12
records for its in-memory broker.

**ElasticMQ is the CI backend.** It is Apache-2.0, needs no account, speaks the
AWS JSON protocol (from 1.4.0; the SDK has spoken only JSON since
`client-sqs` 3.446.0) and runs as a 30 MB native image. LocalStack needs an auth
token since 2026.03.0, and a token is a secret fork PRs cannot read, so a
LocalStack leg could not be a gate that fails on skip. ElasticMQ is not AWS: the
docs state the gaps, and the contract records where it differs.

**Lifecycle-hook order across providers is not a contract.** Nothing in this
package relies on the order of providers within a phase; consumers start in
`onApplicationBootstrap`, after every `onModuleInit`.

**NestJS 12 is ESM-only: never import a directory index from `@nestjs/*`.**
`test/nestjs-deep-imports.spec.ts` requires every deep import to name a file.

## Local Full-Mode Verification (optional infra)

Everything here is opt-in and local-only; the default `npm test` and CI's unit
path run without Docker against `InMemorySqs`.

- `npm run infra:up` starts ElasticMQ from `compose.yaml` on
  `127.0.0.1:19324` and waits for it to answer.
- `npm run test:full` runs the unit suite, then the gated suite against it
  through the strict runner (which fails on any skip).
- `npm run infra:down` removes the container.
- The gated suite reads only `SQS_ENDPOINT` (plus `AWS_REGION` and credentials,
  defaulting to dummy values), so it can point at any SQS-compatible endpoint.

**AI agents working on this repo**: when Docker is available, run
`npm run infra:up && npm run test:full` before opening a PR that touches package
source, and report the result. When it is not, run `npm test` and state that the
gated suite was skipped. Never wire infra into the unit path.
