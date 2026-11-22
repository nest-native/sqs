import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  Type,
} from '@nestjs/common';
import { PARAMTYPES_METADATA } from '@nestjs/common/constants';
import { ApplicationConfig, MetadataScanner, ModuleRef, ModulesContainer, Reflector } from '@nestjs/core';
import { ContextIdFactory } from '@nestjs/core/helpers/context-id-factory';
import { STATIC_CONTEXT } from '@nestjs/core/injector/constants';
import { systemClock } from './clock';
import { SQS_CONSUMER_METADATA, SQS_HANDLER_METADATA } from './constants';
import { SqsConsumerMetadata, SqsHandlerMetadata, SqsModuleOptions } from './interfaces';
import { SqsContextCreator, SqsHandlerContext } from './sqs-context-creator';
import { resolveConsumerOptions } from './sqs-consumer-options';
import { SqsHandlerInvocation } from './sqs-dispatcher';
import { createSqsEnhancerRuntime } from './sqs-enhancer-runtime.factory';
import { defaultSqsErrorMapper } from './sqs-error-mapping';
import { Controller } from './sqs-params.resolver';
import { SqsQueueConsumer } from './sqs-queue-consumer';
import { ResolvedSqsQueue, SqsQueueRegistry } from './sqs-queue-registry';
import { resolveRetryBackoff } from './sqs-retry-backoff';
import { SQS_MODULE_OPTIONS } from './tokens';

/** The slice of Nest's `InstanceWrapper` the explorer reads. */
interface InstanceWrapperLike {
  instance?: unknown;
  metatype?: unknown;
  id?: string;
  isDependencyTreeStatic(): boolean;
}

/** One discovered `@SqsHandler` method. */
interface DiscoveredHandler {
  queueKey: string;
  metadata: SqsHandlerMetadata;
  run: (invocation: SqsHandlerInvocation) => Promise<unknown>;
  /** `Class.method`, to name the handler in errors. */
  describe: string;
}

/** A handler whose queue resolved, ready to become a consumer. */
interface PreparedConsumer {
  handler: DiscoveredHandler;
  queue: ResolvedSqsQueue;
  queueVisibilityTimeout?: number;
}

const DEFAULT_DRAIN_TIMEOUT_MS = 20_000;

/**
 * Discovers `@SqsHandler` methods on every provider and controller, wires each
 * through the Nest enhancer pipeline, and runs one consumer per queue.
 *
 * Consumers start in `onApplicationBootstrap`, once every provider and global
 * enhancer exists. Before any starts, every handler's queue is resolved and
 * read with one `GetQueueAttributes` (`VisibilityTimeout` and
 * `RedrivePolicy`), so a missing queue fails the bootstrap with the handler
 * named.
 *
 * They stop in `beforeApplicationShutdown`: NestJS completes each shutdown
 * phase in every module before it starts the next, so the drain finishes before
 * any `onApplicationShutdown` — where database modules close their pools —
 * whatever the order of providers within a phase.
 *
 * @internal
 */
@Injectable()
export class SqsConsumerExplorer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger('SqsConsumer');
  private readonly contextCreator: SqsContextCreator;
  private readonly consumers: SqsQueueConsumer[] = [];

  constructor(
    private readonly metadataScanner: MetadataScanner,
    private readonly modulesContainer: ModulesContainer,
    private readonly reflector: Reflector,
    applicationConfig: ApplicationConfig,
    private readonly moduleRef: ModuleRef,
    @Inject(SQS_MODULE_OPTIONS) private readonly options: SqsModuleOptions,
    private readonly registry: SqsQueueRegistry,
  ) {
    this.contextCreator = new SqsContextCreator(createSqsEnhancerRuntime(modulesContainer, applicationConfig));
  }

  async onApplicationBootstrap(): Promise<void> {
    const handlers = this.discoverHandlers();
    if (handlers.length === 0) {
      return;
    }
    const prepared = await this.prepare(handlers);
    const errorMapper = this.options.errorMapper ?? defaultSqsErrorMapper;
    const retryBackoff = resolveRetryBackoff(this.options.retryBackoff);
    const clock = this.options.clock ?? systemClock;
    // Every handler's options are resolved, and validated, before any consumer starts.
    const resolved = prepared.map(({ handler, queue, queueVisibilityTimeout }) => ({
      queue,
      run: handler.run,
      consumer: resolveConsumerOptions(
        [this.registry.options(handler.queueKey), handler.metadata.options, this.options.consumerDefaults ?? {}],
        queueVisibilityTimeout,
        handler.describe,
      ),
    }));
    for (const { queue, run, consumer } of resolved) {
      if (consumer.heartbeatOff) {
        this.logger.warn(`The visibility heartbeat of "${queue.key}" is off: ${consumer.heartbeatOff}.`);
      }
      const queueConsumer = new SqsQueueConsumer({
        queue,
        consumer,
        run,
        errorMapper,
        retryBackoff,
        clock,
        logger: this.logger,
      });
      this.consumers.push(queueConsumer);
      queueConsumer.start();
    }
  }

  async beforeApplicationShutdown(): Promise<void> {
    const consumers = this.consumers.splice(0);
    const shutdown = {
      drainTimeoutMs: this.options.shutdown?.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS,
      abortReceive: this.options.shutdown?.abortReceive ?? false,
    };
    await Promise.all(consumers.map(consumer => consumer.stop(shutdown)));
  }

  /** Resolve every handler's queue, refusing two handlers on one queue. */
  private async prepare(handlers: DiscoveredHandler[]): Promise<PreparedConsumer[]> {
    const prepared: PreparedConsumer[] = [];
    const byUrl = new Map<string, DiscoveredHandler>();
    for (const handler of handlers) {
      const queue = await this.resolveQueue(handler);
      const existing = byUrl.get(queue.url);
      if (existing) {
        throw new Error(
          `${existing.describe} and ${handler.describe} both consume ${queue.url}. SQS delivers each ` +
            'message to one receiver: raise the concurrency of one handler instead of adding a second.',
        );
      }
      byUrl.set(queue.url, handler);
      prepared.push({ handler, queue, queueVisibilityTimeout: await this.inspectQueue(handler, queue) });
    }
    return prepared;
  }

  private async resolveQueue(handler: DiscoveredHandler): Promise<ResolvedSqsQueue> {
    let queue: ResolvedSqsQueue;
    try {
      queue = await this.registry.resolve(handler.queueKey);
    } catch (error) {
      throw bootstrapError(handler, `could not resolve the queue "${handler.queueKey}"`, error);
    }
    return queue;
  }

  /**
   * Read `VisibilityTimeout` and `RedrivePolicy` with one call. A queue that
   * does not exist fails the bootstrap; any other failure (no permission, for
   * one) is warned about once and the consumer falls back to its own
   * `visibilityTimeout` option.
   */
  private async inspectQueue(handler: DiscoveredHandler, queue: ResolvedSqsQueue): Promise<number | undefined> {
    let attributes: Partial<Record<string, string>>;
    try {
      attributes = await queue.driver.getQueueAttributes(queue.url, ['VisibilityTimeout', 'RedrivePolicy']);
    } catch (error) {
      if (errorName(error) === 'QueueDoesNotExist') {
        throw bootstrapError(handler, `the queue "${handler.queueKey}" does not exist`, error);
      }
      this.logger.warn(
        `Could not read the attributes of "${handler.queueKey}" (${describe(error)}); the redrive check is ` +
          "skipped, and the consumer's visibility is its visibilityTimeout option (when set).",
      );
      return undefined;
    }
    if (!attributes.RedrivePolicy) {
      this.logger.warn(
        `The queue "${handler.queueKey}" has no RedrivePolicy: a message whose handler keeps failing is ` +
          'retried until its MessageRetentionPeriod expires. Configure a dead-letter queue.',
      );
    }
    const visibility = Number(attributes.VisibilityTimeout);
    return attributes.VisibilityTimeout !== undefined && Number.isInteger(visibility) ? visibility : undefined;
  }

  private discoverHandlers(): DiscoveredHandler[] {
    const handlers: DiscoveredHandler[] = [];
    const seen = new WeakMap<object, Set<string>>();
    for (const [moduleKey, moduleRef] of this.modulesContainer) {
      for (const wrapper of [...moduleRef.providers.values(), ...moduleRef.controllers.values()]) {
        this.collect(wrapper as InstanceWrapperLike, moduleKey, handlers, seen);
      }
    }
    return handlers;
  }

  private collect(
    wrapper: InstanceWrapperLike,
    moduleKey: string,
    handlers: DiscoveredHandler[],
    seen: WeakMap<object, Set<string>>,
  ): void {
    const { instance, metatype } = wrapper;
    if (!instance || typeof metatype !== 'function') {
      return;
    }
    const prototype = Object.getPrototypeOf(instance) as Record<string, unknown>;
    const consumer = this.reflector.get<SqsConsumerMetadata | undefined>(SQS_CONSUMER_METADATA, metatype as Type);
    const methods = seen.get(metatype) ?? new Set<string>();
    seen.set(metatype, methods);
    for (const methodName of this.metadataScanner.getAllMethodNames(prototype)) {
      const metadata = this.reflector.get<SqsHandlerMetadata | undefined>(
        SQS_HANDLER_METADATA,
        prototype[methodName] as Type,
      );
      if (!metadata) {
        continue;
      }
      // A class provided in several modules is one handler, not several.
      if (methods.has(methodName)) {
        continue;
      }
      methods.add(methodName);
      const describeHandler = `${(metatype as Type).name}.${methodName}`;
      const queueKey = metadata.queue ?? consumer?.queue;
      if (!queueKey) {
        throw new Error(
          `${describeHandler} has no queue: name one in @SqsHandler(queue) or @SqsConsumer(queue).`,
        );
      }
      handlers.push({
        queueKey,
        metadata,
        describe: describeHandler,
        run: this.createRunner(wrapper, metatype as Type, prototype, methodName, moduleKey),
      });
    }
  }

  private createRunner(
    wrapper: InstanceWrapperLike,
    metatype: Type,
    prototype: Record<string, unknown>,
    methodName: string,
    moduleKey: string,
  ): (invocation: SqsHandlerInvocation) => Promise<unknown> {
    // Static (singleton) providers reuse their instance; a request-scoped
    // provider gets a fresh context, and instance, for every message.
    const isStatic = wrapper.isDependencyTreeStatic();
    const handler: SqsHandlerContext = {
      callback: prototype[methodName] as (...args: unknown[]) => unknown,
      metatype,
      methodName,
      moduleKey,
      paramTypes: (Reflect.getMetadata(PARAMTYPES_METADATA, prototype, methodName) as unknown[]) ?? [],
      inquirerId: wrapper.id,
      resolveContextId: () => (isStatic ? STATIC_CONTEXT : ContextIdFactory.create()),
      resolveInstance: contextId => this.resolveInstance(wrapper, metatype, contextId),
    };
    return this.contextCreator.create(handler);
  }

  private async resolveInstance(
    wrapper: InstanceWrapperLike,
    metatype: Type,
    contextId: { id: number },
  ): Promise<Controller> {
    if (contextId === STATIC_CONTEXT) {
      return wrapper.instance as Controller;
    }
    return (await this.moduleRef.resolve(metatype, contextId, { strict: false })) as Controller;
  }
}

function bootstrapError(handler: DiscoveredHandler, problem: string, cause: unknown): Error {
  const error = new Error(`${handler.describe}: ${problem} (${describe(cause)}).`);
  (error as { cause?: unknown }).cause = cause;
  return error;
}

function errorName(error: unknown): string | undefined {
  return (error as { name?: string } | null)?.name;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
