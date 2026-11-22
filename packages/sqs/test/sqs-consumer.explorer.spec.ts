import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import {
  ArgumentsHost,
  CallHandler,
  CanActivate,
  Catch,
  Controller,
  ExceptionFilter,
  ExecutionContext,
  Injectable,
  Logger,
  Module,
  NestInterceptor,
  OnApplicationShutdown,
  PipeTransform,
  Provider,
  Scope,
  Type,
  UseFilters,
  UseGuards,
  UseInterceptors,
  UsePipes,
} from '@nestjs/common';
import { APP_GUARD, APP_INTERCEPTOR } from '@nestjs/core';
import { Test, TestingModule } from '@nestjs/testing';
import { map, Observable, of } from 'rxjs';
import { systemClock } from '../clock';
import { SqsReceivedMessage, SqsReceiveRequest } from '../driver';
import { SqsContext } from '../sqs-context';
import { SqsConsumer, SqsHandler } from '../sqs-handler.decorator';
import { SqsAttributes, SqsCtx, SqsMessage, SqsRawMessage } from '../sqs-params.decorators';
import { SqsModule } from '../sqs.module';
import { InMemorySqs } from '../testing/in-memory-sqs';
import { SqsTestModule, SqsTestModuleOptions } from '../testing/sqs-test.module';
import { VirtualClock } from '../testing/virtual-clock';
import { createRecordingLogger, deferred, flush } from './helpers';

const logs = createRecordingLogger();

/** Close the application, moving the virtual clock until it has closed. */
async function closeWhileAdvancing(moduleRef: TestingModule, clock: VirtualClock, stepMs = 250): Promise<void> {
  let closed = false;
  const closing = moduleRef.close().then(() => {
    closed = true;
  });
  for (let step = 0; step < 200 && !closed; step += 1) {
    await clock.advance(stepMs);
  }
  await closing;
}

describe('SqsConsumerExplorer', () => {
beforeEach(() => {
  logs.clear();
  Logger.overrideLogger(logs);
});

interface Booted {
  moduleRef: TestingModule;
  sqs: InMemorySqs;
  clock: VirtualClock;
}

async function boot(
  setup: { providers?: Provider[]; controllers?: Type[]; imports?: unknown[] },
  options: SqsTestModuleOptions = {},
): Promise<Booted> {
  const sqs = options.sqs ?? new InMemorySqs({ autoCreateQueues: true });
  const moduleRef = await Test.createTestingModule({
    imports: [SqsTestModule.forRoot({ ...options, sqs }), ...((setup.imports ?? []) as [])],
    providers: setup.providers ?? [],
    controllers: setup.controllers ?? [],
  })
    .setLogger(logs)
    .compile();
  await moduleRef.init();
  return { moduleRef, sqs, clock: sqs.clock as VirtualClock };
}

/** A queue with a dead-letter queue, so the bootstrap has nothing to warn about. */
function queueWithRedrive(sqs: InMemorySqs, name: string, attributes: Record<string, string> = {}): void {
  sqs.createQueue(`${name}-dlq`);
  sqs.createQueue(name, {
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: sqs.queueArn(`${name}-dlq`), maxReceiveCount: 5 }),
    ...attributes,
  });
}

describe('@SqsHandler discovery and dispatch', () => {
  it('runs a provider method for each message, with the parsed payload, then deletes the message', async () => {
    const received: unknown[] = [];

    @Injectable()
    class OrdersConsumer {
      @SqsHandler('orders')
      placed(@SqsMessage() order: unknown): void {
        received.push(order);
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [OrdersConsumer] });
    sqs.sendMessage('orders', '{"id":1}');
    sqs.sendMessage('orders', 'plain text');
    await sqs.idle();

    assert.deepEqual(received, [{ id: 1 }, 'plain text']);
    assert.deepEqual(sqs.messages('orders'), []);
    await moduleRef.close();
  });

  it('discovers handlers on controllers, and takes the queue from @SqsConsumer', async () => {
    const received: string[] = [];

    @Controller()
    @SqsConsumer('events')
    class EventsController {
      @SqsHandler()
      onEvent(@SqsCtx() context: SqsContext): void {
        received.push(`${context.getQueue().key}:${context.getBody()}`);
      }
    }

    const { moduleRef, sqs } = await boot({ controllers: [EventsController] });
    sqs.sendMessage('events', 'e1');
    await sqs.idle();
    assert.deepEqual(received, ['events:e1']);
    await moduleRef.close();
  });

  it('runs a class provided in two modules once per message', async () => {
    let calls = 0;

    @Injectable()
    class Shared {
      @SqsHandler('shared')
      handle(): void {
        calls += 1;
      }
    }

    @Module({ imports: [SqsModule.forFeature([Shared])] })
    class First {}

    @Module({ providers: [Shared] })
    class Second {}

    const { moduleRef, sqs } = await boot({ imports: [First, Second] });
    sqs.sendMessage('shared', 'x');
    await sqs.idle();
    assert.equal(calls, 1);
    await moduleRef.close();
  });

  it('starts nothing when no handler exists', async () => {
    const { moduleRef, sqs } = await boot({});
    sqs.createQueue('quiet');
    sqs.sendMessage('quiet', 'x');
    await sqs.idle();
    assert.equal(sqs.messages('quiet').length, 1);
    await moduleRef.close();
  });

  it('refuses a handler with no queue, and two handlers on one queue', async () => {
    @Injectable()
    class Nameless {
      @SqsHandler()
      handle(): void {}
    }
    await assert.rejects(boot({ providers: [Nameless] }), /Nameless.handle has no queue/);

    @Injectable()
    class Twice {
      @SqsHandler('orders')
      first(): void {}

      @SqsHandler('orders-alias')
      second(): void {}
    }
    await assert.rejects(
      boot({ providers: [Twice] }, { queues: { 'orders-alias': 'orders' } }),
      /Twice.first and Twice.second both consume .*orders\. SQS delivers each message to one receiver/,
    );
  });
});

describe('the Nest enhancer pipeline', () => {
  it('runs guards, interceptors, pipes and filters around the handler', async () => {
    const seen: string[] = [];

    @Injectable()
    class AllowEvenIds implements CanActivate {
      canActivate(context: ExecutionContext): boolean {
        seen.push(`guard:${context.getType()}`);
        return (context.switchToRpc().getData<{ id: number }>().id % 2) === 0;
      }
    }

    @Injectable()
    class Timing implements NestInterceptor {
      intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
        seen.push('before');
        return next.handle().pipe(map(value => {
          seen.push(`after:${String(value)}`);
          return value;
        }));
      }
    }

    class Double implements PipeTransform {
      transform(value: number): number {
        return value * 2;
      }
    }

    @Catch()
    class Swallow implements ExceptionFilter {
      catch(exception: Error, host: ArgumentsHost): string {
        seen.push(`filter:${exception.message}:${host.getType()}`);
        return 'handled';
      }
    }

    @Injectable()
    class Pipeline {
      @SqsHandler('pipeline')
      @UseGuards(new AllowEvenIds())
      @UseInterceptors(new Timing())
      @UseFilters(new Swallow())
      handle(@SqsMessage('id', new Double()) doubled: number): string {
        if (doubled === 8) {
          throw new Error('four is unlucky');
        }
        seen.push(`handler:${doubled}`);
        return `done:${doubled}`;
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [Pipeline] });
    sqs.sendMessage('pipeline', '{"id":2}');
    await sqs.idle();
    sqs.sendMessage('pipeline', '{"id":4}');
    await sqs.idle();
    sqs.sendMessage('pipeline', '{"id":3}');
    await sqs.idle();

    assert.deepEqual(seen, [
      'guard:rpc',
      'before',
      'handler:4',
      'after:done:4',
      'guard:rpc',
      'before',
      'filter:four is unlucky:rpc',
      'guard:rpc',
      'filter:Forbidden:rpc',
    ]);
    assert.deepEqual(sqs.messages('pipeline'), [], 'a filter that handles an error settles the message as handled');
    await moduleRef.close();
  });

  it('runs method-level pipes over the payload, with and without parameter decorators', async () => {
    const seen: unknown[] = [];

    class Upper implements PipeTransform {
      transform(value: unknown): unknown {
        return typeof value === 'string' ? value.toUpperCase() : value;
      }
    }

    @Injectable()
    class Piped {
      @SqsHandler('piped-decorated')
      @UsePipes(new Upper())
      decorated(@SqsMessage() body: string, @SqsCtx() context: SqsContext): void {
        seen.push(body, context.getQueue().key);
      }

      @SqsHandler('piped-positional')
      @UsePipes(new Upper())
      positional(body: string): void {
        seen.push(body);
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [Piped] });
    sqs.sendMessage('piped-decorated', 'hello');
    await sqs.idle();
    sqs.sendMessage('piped-positional', 'world');
    await sqs.idle();
    assert.deepEqual(seen, ['HELLO', 'piped-decorated', 'WORLD']);
    await moduleRef.close();
  });

  it('handles a method without reflected parameter types, as plain JavaScript produces', async () => {
    const seen: unknown[] = [];

    @Injectable()
    class Untyped {
      @SqsHandler('untyped')
      handle(payload: unknown): void {
        seen.push(payload);
      }
    }

    Reflect.deleteMetadata('design:paramtypes', Untyped.prototype, 'handle');
    const { moduleRef, sqs } = await boot({ providers: [Untyped] });
    sqs.sendMessage('untyped', '{"ok":true}');
    await sqs.idle();
    assert.deepEqual(seen, [{ ok: true }]);
    await moduleRef.close();
  });

  it('runs global guards and interceptors', async () => {
    const seen: string[] = [];

    @Injectable()
    class GlobalGuard implements CanActivate {
      canActivate(): boolean {
        seen.push('global-guard');
        return true;
      }
    }

    @Injectable()
    class GlobalInterceptor implements NestInterceptor {
      intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
        seen.push('global-interceptor');
        return next.handle();
      }
    }

    @Injectable()
    class Handler {
      @SqsHandler('global')
      handle(): Observable<string> {
        seen.push('handler');
        return of('a', 'b');
      }
    }

    const { moduleRef, sqs } = await boot({
      providers: [
        Handler,
        { provide: APP_GUARD, useClass: GlobalGuard },
        { provide: APP_INTERCEPTOR, useClass: GlobalInterceptor },
      ],
    });
    sqs.sendMessage('global', 'x');
    await sqs.idle();
    assert.deepEqual(seen, ['global-guard', 'global-interceptor', 'handler']);
    await moduleRef.close();
  });

  it('resolves a request-scoped handler per message', async () => {
    const instances = new Set<object>();

    @Injectable({ scope: Scope.REQUEST })
    class PerMessage {
      @SqsHandler('scoped')
      handle(): void {
        instances.add(this);
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [PerMessage] });
    sqs.sendMessage('scoped', 'a');
    sqs.sendMessage('scoped', 'b');
    await sqs.idle();
    assert.equal(instances.size, 2);
    await moduleRef.close();
  });

  it('calls the method the instance holds at dispatch time, so later wrapping applies', async () => {
    const seen: string[] = [];

    @Injectable()
    class Wrapped {
      @SqsHandler('wrapped')
      handle(@SqsMessage() body: string): void {
        seen.push(`original:${body}`);
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [Wrapped] });
    const instance = moduleRef.get(Wrapped);
    const original = instance.handle.bind(instance);
    instance.handle = (body: string) => {
      seen.push('wrapper');
      original(body);
    };
    sqs.sendMessage('wrapped', 'x');
    await sqs.idle();
    assert.deepEqual(seen, ['wrapper', 'original:x']);

    // An instance that no longer holds a function there falls back to the
    // method discovered at bootstrap.
    (instance as unknown as Record<string, unknown>).handle = 'not a function';
    sqs.sendMessage('wrapped', 'y');
    await sqs.idle();
    assert.deepEqual(seen, ['wrapper', 'original:x', 'original:y']);
    await moduleRef.close();
  });

  it('injects the payload property, attributes, context and raw message, or positional arguments', async () => {
    const seen: unknown[] = [];

    @Injectable()
    class Params {
      @SqsHandler('params')
      decorated(
        @SqsMessage('id') id: unknown,
        @SqsMessage('id') _again: unknown,
        @SqsAttributes() attributes: unknown,
        @SqsAttributes('type') type: unknown,
        @SqsCtx() context: SqsContext,
        @SqsRawMessage() raw: SqsReceivedMessage,
      ): void {
        seen.push(id, attributes, type, context.getMessageId() === raw.MessageId);
      }

      @SqsHandler('positional')
      positional(payload: unknown, context: SqsContext): void {
        seen.push(payload, context.getQueue().key);
      }

      @SqsHandler('text')
      text(@SqsMessage('id') id: unknown): void {
        seen.push(id);
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [Params] });
    sqs.sendMessage('params', '{"id":7}', { attributes: { type: 'OrderPlaced' } });
    await sqs.idle();
    sqs.sendMessage('positional', '[1,2]');
    await sqs.idle();
    sqs.sendMessage('text', 'not an object');
    await sqs.idle();
    assert.deepEqual(seen, [7, { type: 'OrderPlaced' }, 'OrderPlaced', true, [1, 2], 'positional', undefined]);
    await moduleRef.close();
  });
});

describe('errors and retries', () => {
  it('retries a failure after a backoff that grows with the receive count, then succeeds', async () => {
    const attempts: number[] = [];

    @Injectable()
    class Flaky {
      @SqsHandler('flaky')
      handle(@SqsCtx() context: SqsContext): void {
        attempts.push(context.getReceiveCount());
        if (attempts.length < 3) {
          throw new Error('downstream timeout');
        }
      }
    }

    const prepared = new InMemorySqs();
    queueWithRedrive(prepared, 'flaky');
    const { moduleRef, sqs, clock } = await boot(
      { providers: [Flaky] },
      { sqs: prepared, retryBackoff: { jitter: 'none', initialDelaySeconds: 2 } },
    );
    sqs.sendMessage('flaky', 'x');
    await sqs.idle();
    assert.deepEqual(attempts, [1]);
    assert.equal(sqs.messages('flaky')[0].visible, false);

    await clock.advance(1_999);
    assert.deepEqual(attempts, [1], 'not before the 2 s backoff');
    await clock.advance(1);
    await sqs.idle();
    assert.deepEqual(attempts, [1, 2]);

    await clock.advance(4_000);
    await sqs.idle();
    assert.deepEqual(attempts, [1, 2, 3]);
    assert.deepEqual(sqs.messages('flaky'), []);
    assert.ok(logs.warnings.some(warning => /failed: downstream timeout; retrying it/.test(warning)));
    await moduleRef.close();
  });

  it('deletes a failure the error mapper maps to delete', async () => {
    @Injectable()
    class Poison {
      @SqsHandler('poison')
      handle(): void {
        throw new TypeError('unparseable');
      }
    }

    const { moduleRef, sqs } = await boot(
      { providers: [Poison] },
      { errorMapper: error => (error instanceof TypeError ? 'delete' : 'retry') },
    );
    sqs.sendMessage('poison', 'x');
    await sqs.idle();
    assert.deepEqual(sqs.messages('poison'), []);
    await moduleRef.close();
  });

  it('leaves the visibility alone with retryBackoff: false', async () => {
    @Injectable()
    class Failing {
      @SqsHandler('failing')
      handle(): void {
        throw new Error('x');
      }
    }

    const prepared = new InMemorySqs();
    prepared.createQueue('failing', { VisibilityTimeout: '30' });
    const { moduleRef, sqs, clock } = await boot({ providers: [Failing] }, { sqs: prepared, retryBackoff: false });
    sqs.sendMessage('failing', 'x');
    // Nothing settles the message, so idle() would wait for its visibility to expire.
    await flush(20);
    await clock.advance(29_000);
    assert.equal(sqs.messages('failing')[0].visible, false, 'hidden for the queue visibility timeout');
    await moduleRef.close();
  });
});

describe('bootstrap', () => {
  @Injectable()
  class Orders {
    @SqsHandler('orders')
    handle(): void {}
  }

  function spySqs(sqs: InMemorySqs) {
    const attributeCalls: string[][] = [];
    const receives: SqsReceiveRequest[] = [];
    const factory = sqs.createDriverFactory();
    const driverFactory = (...args: Parameters<typeof factory>) => {
      const driver = factory(...args);
      return {
        ...driver,
        getQueueAttributes: (url: string, names: string[]) => {
          attributeCalls.push(names);
          return driver.getQueueAttributes(url, names);
        },
        receiveMessages: (request: SqsReceiveRequest, options?: { signal?: AbortSignal }) => {
          receives.push(request);
          return driver.receiveMessages(request, options);
        },
      };
    };
    return { attributeCalls, receives, driverFactory };
  }

  async function bootWith(sqs: InMemorySqs, driverFactory: unknown, extra: Record<string, unknown> = {}) {
    const moduleRef = await Test.createTestingModule({
      imports: [
        SqsModule.forRoot({
          driverFactory: driverFactory as never,
          clock: sqs.clock,
          shutdown: { abortReceive: true },
          ...extra,
        }),
      ],
      providers: [Orders],
    })
      .setLogger(logs)
      .compile();
    await moduleRef.init();
    await flush();
    return moduleRef;
  }

  it('reads VisibilityTimeout and RedrivePolicy with one call, and warns about a queue with no dead-letter queue', async () => {
    const sqs = new InMemorySqs();
    sqs.createQueue('orders');
    const spy = spySqs(sqs);
    const moduleRef = await bootWith(sqs, spy.driverFactory);
    assert.deepEqual(spy.attributeCalls, [['VisibilityTimeout', 'RedrivePolicy']]);
    assert.ok(logs.warnings.some(warning => /The queue "orders" has no RedrivePolicy/.test(warning)));
    assert.equal(spy.receives[0].visibilityTimeout, undefined, "the queue's own visibility applies");
    await moduleRef.close();
  });

  it('warns once and falls back to the visibilityTimeout option when the attributes cannot be read', async () => {
    const sqs = new InMemorySqs();
    queueWithRedrive(sqs, 'orders');
    sqs.deny('GetQueueAttributes');
    const spy = spySqs(sqs);
    const moduleRef = await bootWith(sqs, spy.driverFactory, { queues: { orders: { name: 'orders', visibilityTimeout: 90 } } });
    const warnings = logs.warnings.filter(warning => /Could not read the attributes of "orders"/.test(warning));
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /AccessDenied|denied/);
    assert.equal(spy.receives[0].visibilityTimeout, 90, 'the option is passed on every receive');
    await moduleRef.close();
  });

  it('stays quiet about a queue that has a dead-letter queue', async () => {
    const sqs = new InMemorySqs();
    queueWithRedrive(sqs, 'orders');
    const moduleRef = await bootWith(sqs, sqs.createDriverFactory());
    assert.deepEqual(
      logs.warnings.filter(warning => /RedrivePolicy|Could not read/.test(warning)),
      [],
    );
    await moduleRef.close();
  });

  it('fails when the queue does not exist, by name or by URL, naming the handler', async () => {
    const sqs = new InMemorySqs();
    await assert.rejects(bootWith(sqs, sqs.createDriverFactory()), /Orders.handle: could not resolve the queue "orders" \(The specified queue does not exist/);
    await assert.rejects(
      bootWith(sqs, sqs.createDriverFactory(), { queues: { orders: 'http://sqs.in-memory.test/000000000000/orders' } }),
      /Orders.handle: the queue "orders" does not exist/,
    );
  });

  it('reports a lookup failure that is not an Error', async () => {
    const sqs = new InMemorySqs();
    const factory = sqs.createDriverFactory();
    await assert.rejects(
      bootWith(sqs, (...args: Parameters<typeof factory>) => ({
        ...factory(...args),
        getQueueUrl: async () => {
          throw 'throttled';
        },
      })),
      /Orders.handle: could not resolve the queue "orders" \(throttled\)\./,
    );
  });

  it('runs on the system clock when no clock is configured', async () => {
    const sqs = new InMemorySqs({ clock: systemClock });
    queueWithRedrive(sqs, 'orders');
    let handled = 0;

    @Injectable()
    class RealTime {
      @SqsHandler('orders')
      handle(): void {
        handled += 1;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [SqsModule.forRoot({ driverFactory: sqs.createDriverFactory(), shutdown: { abortReceive: true } })],
      providers: [RealTime],
    })
      .setLogger(logs)
      .compile();
    await moduleRef.init();
    sqs.sendMessage('orders', 'x');
    await sqs.idle();
    assert.equal(handled, 1);
    await moduleRef.close();
  });

  it('refuses invalid consumer options', async () => {
    const sqs = new InMemorySqs();
    queueWithRedrive(sqs, 'orders');
    await assert.rejects(
      bootWith(sqs, sqs.createDriverFactory(), { consumerDefaults: { waitTimeSeconds: 0 } }),
      /Invalid consumer options for Orders.handle: waitTimeSeconds must be/,
    );
    await assert.rejects(
      bootWith(sqs, sqs.createDriverFactory(), { consumerDefaults: { heartbeat: { intervalSeconds: 30 } } }),
      /Invalid consumer options for Orders.handle: heartbeat.intervalSeconds \(30\) must be shorter than the visibility timeout \(30 s\)/,
    );
  });

  it('turns the heartbeat off with one warning when the visibility timeout is unknown, and quietly on request', async () => {
    const sqs = new InMemorySqs();
    queueWithRedrive(sqs, 'orders');
    sqs.deny('GetQueueAttributes');
    const moduleRef = await bootWith(sqs, sqs.createDriverFactory());
    const off = logs.warnings.filter(warning => /visibility heartbeat/.test(warning));
    assert.deepEqual(off, [
      'The visibility heartbeat of "orders" is off: the visibility timeout is unknown (the queue\'s attributes ' +
        'could not be read and visibilityTimeout is not set); set visibilityTimeout to turn it on.',
    ]);
    await moduleRef.close();

    logs.clear();
    const quiet = await bootWith(sqs, sqs.createDriverFactory(), { consumerDefaults: { heartbeat: false } });
    assert.deepEqual(
      logs.warnings.filter(warning => /visibility heartbeat/.test(warning)),
      [],
    );
    await quiet.close();
  });

  it('accepts a queue whose attributes leave the visibility out or garble it', async () => {
    for (const attributes of [{ RedrivePolicy: '{}' }, { RedrivePolicy: '{}', VisibilityTimeout: 'thirty' }]) {
      const sqs = new InMemorySqs();
      queueWithRedrive(sqs, 'orders');
      const factory = sqs.createDriverFactory();
      const moduleRef = await bootWith(sqs, (...args: Parameters<typeof factory>) => ({
        ...factory(...args),
        getQueueAttributes: async () => attributes,
      }));
      await moduleRef.close();
    }
  });
});

describe('FIFO queues', () => {
  function sendAll(sqs: InMemorySqs, queue: string, messages: [string, string][]): void {
    for (const [body, groupId] of messages) {
      sqs.sendMessage(queue, body, { groupId, deduplicationId: body });
    }
  }

  it('runs one message of a group at a time, in order, and different groups at once', async () => {
    const events: string[] = [];
    const active = new Map<string, number>();
    let mostAtOnceInAGroup = 0;

    @Injectable()
    class Ledger {
      @SqsHandler('ledger', { concurrency: 4 })
      async post(@SqsMessage() body: string, @SqsCtx() context: SqsContext): Promise<void> {
        const group = context.getGroupId() as string;
        active.set(group, (active.get(group) ?? 0) + 1);
        mostAtOnceInAGroup = Math.max(mostAtOnceInAGroup, active.get(group) as number);
        events.push(`start:${body}`);
        await flush(2);
        events.push(`end:${body}`);
        active.set(group, (active.get(group) as number) - 1);
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [Ledger] }, { queues: { ledger: 'ledger.fifo' } });
    sendAll(sqs, 'ledger.fifo', [
      ['a1', 'a'],
      ['b1', 'b'],
      ['a2', 'a'],
      ['b2', 'b'],
      ['a3', 'a'],
    ]);
    await sqs.idle();

    const finished = (group: string) => events.filter(event => event.startsWith(`end:${group}`));
    assert.deepEqual(finished('a'), ['end:a1', 'end:a2', 'end:a3']);
    assert.deepEqual(finished('b'), ['end:b1', 'end:b2']);
    assert.equal(mostAtOnceInAGroup, 1);
    assert.ok(events.indexOf('start:b1') < events.indexOf('end:a1'), 'the groups ran at the same time');
    assert.deepEqual(sqs.messages('ledger.fifo'), []);
    await moduleRef.close();
  });

  it('redelivers a failed message before the ones behind it, without holding other groups', async () => {
    const handled: string[] = [];

    @Injectable()
    class Ledger {
      @SqsHandler('ledger', { concurrency: 4 })
      post(@SqsMessage() body: string, @SqsCtx() context: SqsContext): void {
        handled.push(`${body}#${context.getReceiveCount()}`);
        if (body === 'a2' && context.getReceiveCount() === 1) {
          throw new Error('a2 failed');
        }
      }
    }

    const { moduleRef, sqs, clock } = await boot(
      { providers: [Ledger] },
      { queues: { ledger: 'ledger.fifo' }, retryBackoff: { initialDelaySeconds: 2, jitter: 'none' } },
    );
    sendAll(sqs, 'ledger.fifo', [
      ['a1', 'a'],
      ['b1', 'b'],
      ['a2', 'a'],
      ['b2', 'b'],
      ['a3', 'a'],
    ]);
    await sqs.idle();
    assert.deepEqual(
      handled.filter(entry => entry.startsWith('b')),
      ['b1#1', 'b2#1'],
      'group b finished while a2 waits out its backoff',
    );
    assert.deepEqual(
      handled.filter(entry => entry.startsWith('a')),
      ['a1#1', 'a2#1'],
    );
    const a3 = sqs.messages('ledger.fifo').find(message => message.body === 'a3');
    assert.equal(a3?.visible, true, 'a3 was released, not run');

    await clock.advance(2_000);
    await sqs.idle();
    assert.deepEqual(
      handled.filter(entry => entry.startsWith('a')),
      ['a1#1', 'a2#1', 'a2#2', 'a3#2'],
    );
    assert.deepEqual(sqs.messages('ledger.fifo'), []);
    await moduleRef.close();
  });

  it('releases the messages waiting behind a running one when shutdown begins, then drains it', async () => {
    const gate = deferred<void>();
    const handled: string[] = [];

    @Injectable()
    class Ledger {
      @SqsHandler('ledger', { concurrency: 3 })
      async post(@SqsMessage() body: string): Promise<void> {
        handled.push(body);
        await gate.promise;
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [Ledger] }, { queues: { ledger: 'ledger.fifo' } });
    sendAll(sqs, 'ledger.fifo', [
      ['g1', 'g'],
      ['g2', 'g'],
      ['g3', 'g'],
    ]);
    await flush();
    assert.deepEqual(handled, ['g1']);

    const closing = moduleRef.close();
    await flush(20);
    assert.deepEqual(
      sqs.messages('ledger.fifo').map(message => [message.body, message.inFlight, message.visible]),
      [
        ['g1', true, false],
        ['g2', false, true],
        ['g3', false, true],
      ],
      'released while g1 still runs',
    );
    gate.resolve();
    await closing;
    assert.deepEqual(handled, ['g1']);
    assert.deepEqual(
      sqs.messages('ledger.fifo').map(message => message.body),
      ['g2', 'g3'],
      'g1 finished and was deleted; g2 and g3 wait for another consumer',
    );
  });
});

describe('the visibility heartbeat', () => {
  async function until(condition: () => boolean): Promise<void> {
    for (let round = 0; round < 100 && !condition(); round += 1) {
      await flush();
    }
    assert.ok(condition(), 'the condition never held');
  }

  it('keeps the message of a long handler, and the ones waiting behind it, invisible', async () => {
    const gate = deferred<void>();
    const handled: string[] = [];

    @Injectable()
    class Slow {
      @SqsHandler('slow', { concurrency: 2 })
      async handle(@SqsMessage() body: string, @SqsCtx() context: SqsContext): Promise<void> {
        handled.push(`${body}#${context.getReceiveCount()}`);
        if (body === 's1') {
          await gate.promise;
        }
      }
    }

    const prepared = new InMemorySqs();
    prepared.createQueue('slow.fifo', { FifoQueue: 'true', VisibilityTimeout: '10' });
    const { moduleRef, sqs, clock } = await boot({ providers: [Slow] }, { sqs: prepared, queues: { slow: 'slow.fifo' } });
    sqs.sendMessage('slow.fifo', 's1', { groupId: 's', deduplicationId: 's1' });
    sqs.sendMessage('slow.fifo', 's2', { groupId: 's', deduplicationId: 's2' });
    await flush();
    for (let second = 0; second < 60; second += 1) {
      await clock.advance(1_000);
    }
    assert.deepEqual(
      sqs.messages('slow.fifo').map(message => [message.body, message.visible, message.receiveCount]),
      [
        ['s1', false, 1],
        ['s2', false, 1],
      ],
      'a minute later, six times the visibility timeout, neither is visible',
    );
    assert.deepEqual(handled, ['s1#1']);

    gate.resolve();
    await until(() => sqs.messages('slow.fifo').length === 0);
    assert.deepEqual(handled, ['s1#1', 's2#1']);
    await moduleRef.close();
  });

  it('leaves the message of a long handler to become visible again when turned off', async () => {
    const gate = deferred<void>();

    @Injectable()
    class Slow {
      @SqsHandler('slow', { heartbeat: false })
      async handle(): Promise<void> {
        await gate.promise;
      }
    }

    const prepared = new InMemorySqs();
    queueWithRedrive(prepared, 'slow', { VisibilityTimeout: '10' });
    const { moduleRef, sqs, clock } = await boot({ providers: [Slow] }, { sqs: prepared });
    sqs.sendMessage('slow', 'x');
    await flush();
    await clock.advance(10_000);
    assert.equal(sqs.messages('slow')[0].visible, true);
    gate.resolve();
    await until(() => sqs.messages('slow').length === 0);
    await moduleRef.close();
  });
});

describe('shutdown', () => {
  it('drains in beforeApplicationShutdown, before any onApplicationShutdown runs', async () => {
    const order: string[] = [];
    const gate = deferred<void>();

    @Injectable()
    class Database implements OnApplicationShutdown {
      onApplicationShutdown(): void {
        order.push('database closed');
      }
    }

    @Injectable()
    class Slow {
      constructor(private readonly database: Database) {}

      @SqsHandler('slow')
      async handle(): Promise<void> {
        order.push('handler started');
        await gate.promise;
        order.push(this.database ? 'handler finished' : 'no database');
      }
    }

    const { moduleRef, sqs } = await boot({ providers: [Slow, Database] });
    sqs.sendMessage('slow', 'x');
    await flush();
    const closing = moduleRef.close();
    await flush();
    gate.resolve();
    await closing;

    assert.deepEqual(order, ['handler started', 'handler finished', 'database closed']);
    assert.deepEqual(sqs.messages('slow'), [], 'the drained message was deleted before shutdown went on');
  });

  it('waits for the long poll in flight by default, and releases what it returns', async () => {
    let handled = 0;

    @Injectable()
    class Idle {
      @SqsHandler('idle')
      handle(): void {
        handled += 1;
      }
    }

    const prepared = new InMemorySqs();
    queueWithRedrive(prepared, 'idle');
    const { moduleRef, sqs } = await boot({ providers: [Idle] }, { sqs: prepared, shutdown: { abortReceive: false } });
    await flush();
    const closing = moduleRef.close();
    await flush(50);
    sqs.sendMessage('idle', 'arrived during shutdown');
    await closing;

    assert.equal(handled, 0);
    const [message] = sqs.messages('idle');
    assert.equal(message.receiveCount, 1, 'the long poll returned it');
    assert.equal(message.visible, true, 'and it was released at once, not left until its visibility expired');
  });

  it('releases what outlives the drain timeout, and aborts its signal', async () => {
    let signal!: AbortSignal;

    @Injectable()
    class Stuck {
      @SqsHandler('stuck')
      async handle(@SqsCtx() context: SqsContext): Promise<void> {
        signal = context.signal;
        await new Promise(() => undefined);
      }
    }

    const prepared = new InMemorySqs();
    queueWithRedrive(prepared, 'stuck');
    const { moduleRef, sqs, clock } = await boot({ providers: [Stuck] }, { sqs: prepared, shutdown: { drainTimeoutMs: 1_000 } });
    sqs.sendMessage('stuck', 'x');
    await flush();
    await closeWhileAdvancing(moduleRef, clock);

    assert.equal(signal.aborted, true);
    assert.equal(sqs.messages('stuck')[0].visible, true);
    assert.ok(logs.warnings.some(warning => /still being handled after 1000 ms/.test(warning)));
  });
});
});
