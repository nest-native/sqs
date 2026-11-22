import { ModuleMetadata, Provider } from '@nestjs/common';
import { SqsClock } from './clock';
import { SqsClientInput, SqsDriverFactory } from './driver';
import { SqsErrorMapper } from './sqs-error-mapping';
import { SqsRetryBackoffOptions } from './sqs-retry-backoff';

/**
 * How one consumer polls and processes its queue. Set it per handler on
 * `@SqsHandler`, per queue under `queues`, or for every consumer under
 * `consumerDefaults`; the queue's configuration wins over the decorator, which
 * wins over the defaults.
 */
export interface SqsConsumerOptions {
  /**
   * How many messages this consumer holds at once, from receive to settle. A
   * receive never asks for more messages than there are free slots, so no
   * message waits in memory while its visibility timeout runs.
   *
   * The default, 1, processes one message at a time — the effective default of
   * `@ssut/nestjs-sqs` (`batchSize: 1`). Raise it for throughput; your
   * handlers then run concurrently.
   *
   * @default 1
   */
  concurrency?: number;

  /**
   * The most messages one receive asks for (`MaxNumberOfMessages`, 1 to 10).
   * Each receive asks for the smaller of this and the free slots.
   *
   * @default 10
   */
  maxMessages?: number;

  /**
   * How long a receive long-polls an empty queue (`WaitTimeSeconds`, 1 to 20).
   * Short polling (0) is not accepted: it turns an empty queue into a tight
   * loop of empty receives.
   *
   * @default 20
   */
  waitTimeSeconds?: number;

  /**
   * How long a receive may take before it is aborted and retried. It must
   * exceed the long poll: AWS asks for an HTTP timeout longer than
   * `WaitTimeSeconds`, and the SDK sets none by default.
   *
   * @default waitTimeSeconds * 1000 + 10_000
   */
  receiveTimeoutMs?: number;

  /**
   * A per-receive `VisibilityTimeout`, in seconds (0 to 43 200). Unset, the
   * queue's own `VisibilityTimeout` applies; it is read once at bootstrap.
   */
  visibilityTimeout?: number;

  /**
   * The visibility heartbeat: while the consumer holds a message — running its
   * handler, or, on a FIFO queue, waiting behind an earlier message of its
   * group — it extends the message's visibility, so the message does not
   * become visible to other consumers in the middle of its processing.
   * `false` turns it off.
   *
   * It needs the visibility timeout: when the queue's attributes cannot be
   * read and `visibilityTimeout` is not set, the heartbeat is off, with one
   * warning, and so it is when the visibility timeout is 0 or 1 second, too
   * short to extend in time.
   *
   * @default {} (on, with the defaults of {@link SqsHeartbeatOptions})
   */
  heartbeat?: SqsHeartbeatOptions | false;
}

/**
 * When the visibility heartbeat extends a held message, and by how much. The
 * first extension comes `intervalSeconds` after the receive, so a handler that
 * finishes sooner costs no call. No extension passes the 12 hours SQS allows
 * from a receive: the one that reaches that limit is the last, with a warning.
 */
export interface SqsHeartbeatOptions {
  /**
   * Seconds between two extensions of one held message. It must be shorter
   * than the visibility timeout and than `extendBySeconds`.
   *
   * @default max(1, floor(visibility timeout / 2))
   */
  intervalSeconds?: number;

  /**
   * The visibility timeout each extension sets, counted from the extension
   * (1 to 43 200 seconds).
   *
   * @default the consumer's visibility timeout
   */
  extendBySeconds?: number;
}

/**
 * A logical queue. Handlers and `SqsProducerService` refer to queues by key, so
 * the physical name, URL and account come from configuration — never from
 * `process.env` read while a decorator is evaluated.
 */
export interface SqsQueueOptions extends SqsConsumerOptions {
  /** The queue's name, resolved with `GetQueueUrl` (and cached). */
  name?: string;
  /** The queue's full URL, used as is. Wins over `name`. */
  url?: string;
  /** The account that owns the queue, for a cross-account `GetQueueUrl`. */
  ownerAccountId?: string;
  /** The named client (see `clients`) that talks to this queue. */
  client?: string;
}

/** What graceful shutdown does. It runs in `beforeApplicationShutdown`. */
export interface SqsShutdownOptions {
  /**
   * How long in-flight handlers may run once shutdown begins. A message whose
   * handler is still running then is released (visibility 0) for another
   * consumer, and its `signal` aborts.
   *
   * @default 20_000
   */
  drainTimeoutMs?: number;

  /**
   * Abort the long poll in flight instead of waiting for it to return. By
   * default shutdown waits for it — at most its receive deadline — and releases
   * whatever it returned, so no message is stranded until its visibility
   * timeout expires. Aborting is faster, at that cost.
   *
   * @default false
   */
  abortReceive?: boolean;
}

/** Producer settings. */
export interface SqsProducerOptions {
  /**
   * The largest message `SqsProducerService` sends, counting the body and the
   * attributes. Lower it to match a queue whose `MaximumMessageSize` is set
   * lower; SQS's own error passes through otherwise.
   *
   * @default 1_048_576 (1 MiB)
   */
  maxMessageBytes?: number;
}

/**
 * Configuration for {@link SqsModule.forRoot}.
 */
export interface SqsModuleOptions {
  /**
   * Register the module globally, so the producer and the clients are
   * injectable everywhere.
   *
   * @default true
   */
  isGlobal?: boolean;

  /**
   * The default client: an `SQSClient` configuration (`region`, `endpoint`,
   * `credentials`, …) or an existing client. Unset, the SDK's defaults apply.
   * For an emulator, set `endpoint` (with a region and dummy credentials).
   */
  client?: SqsClientInput;

  /**
   * Named clients, for queues in other regions or accounts. A queue picks one
   * with `queues.<key>.client`. `default` is reserved for `client`.
   */
  clients?: Record<string, SqsClientInput>;

  /**
   * Logical queues by key. A string is a queue URL when it starts with
   * `http://` or `https://`, and a queue name otherwise. A key that is not
   * configured is taken as a queue name on the default client.
   */
  queues?: Record<string, string | SqsQueueOptions>;

  /** Consumer settings every queue inherits. */
  consumerDefaults?: SqsConsumerOptions;

  /**
   * What happens to a message whose handler threw an error no exception filter
   * handled. Defaults to {@link defaultSqsErrorMapper}, which retries every
   * error: the queue's redrive policy is the dead-letter mechanism.
   */
  errorMapper?: SqsErrorMapper;

  /**
   * How long a retried message stays invisible: `initialDelaySeconds`, growing
   * by `multiplier` with each receive (`ApproximateReceiveCount`), up to
   * `maxDelaySeconds`, with jitter. `false` leaves the visibility untouched, so
   * the message returns when its visibility timeout expires.
   *
   * @default { initialDelaySeconds: 1, maxDelaySeconds: 300, multiplier: 2, jitter: 'equal' }
   */
  retryBackoff?: SqsRetryBackoffOptions | false;

  /** Graceful shutdown settings. */
  shutdown?: SqsShutdownOptions;

  /** Producer settings. */
  producer?: SqsProducerOptions;

  /**
   * Advanced: replace the driver factory, for tests or a custom client.
   * `SqsTestModule` sets it to an `InMemorySqs`.
   */
  driverFactory?: SqsDriverFactory;

  /**
   * Advanced: the time source for deadlines and backoffs. `SqsTestModule` sets
   * it to the `InMemorySqs` virtual clock.
   */
  clock?: SqsClock;
}

/**
 * Configuration for {@link SqsModule.forRootAsync}.
 */
export interface SqsModuleAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  /** @default true */
  isGlobal?: boolean;
  /** Providers to inject into `useFactory`. */
  inject?: any[];
  /** Additional providers registered alongside the resolved options. */
  extraProviders?: Provider[];
  /** Resolves the {@link SqsModuleOptions}. */
  useFactory: (...args: any[]) => SqsModuleOptions | Promise<SqsModuleOptions>;
}

/** Options accepted by {@link SqsHandler}: code-level consumer defaults. */
export type SqsHandlerOptions = SqsConsumerOptions;

/** Metadata stored on an `@SqsHandler` method. */
export interface SqsHandlerMetadata {
  /** The logical queue. Falls back to the class's `@SqsConsumer` queue. */
  queue?: string;
  options: SqsHandlerOptions;
}

/** Metadata stored on an `@SqsConsumer` class. */
export interface SqsConsumerMetadata {
  /** The default queue for the class's handlers. */
  queue?: string;
}
