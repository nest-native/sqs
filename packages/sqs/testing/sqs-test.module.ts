import { DynamicModule, Module } from '@nestjs/common';
import { SqsModuleOptions } from '../interfaces';
import { SqsModule } from '../sqs.module';
import { InMemorySqs } from './in-memory-sqs';

/** Options for {@link SqsTestModule.forRoot}. */
export type SqsTestModuleOptions = Omit<SqsModuleOptions, 'driverFactory' | 'clock'> & {
  /**
   * The SQS to run against. Defaults to a fresh `InMemorySqs` that creates a
   * queue the first time a name is resolved.
   */
  sqs?: InMemorySqs;
};

/**
 * `SqsModule` wired to an {@link InMemorySqs}: every handler, the producer and
 * the error mapping run as in production, against queues in memory and on the
 * virtual clock. Inject `InMemorySqs` to put messages on queues, move time and
 * inspect what happened.
 *
 * Shutdown aborts the long poll in flight (`shutdown.abortReceive`), so closing
 * a test application never waits on the virtual clock; pass
 * `shutdown: { abortReceive: false }` to test the production behaviour.
 *
 * @example
 * ```ts
 * const moduleRef = await Test.createTestingModule({
 *   imports: [SqsTestModule.forRoot({ queues: { orders: 'orders' } })],
 *   providers: [OrdersConsumer],
 * }).compile();
 * await moduleRef.init();
 * const sqs = moduleRef.get(InMemorySqs);
 * sqs.sendMessage('orders', JSON.stringify({ id: 1 }));
 * await sqs.idle();
 * ```
 *
 * @publicApi
 */
@Module({})
export class SqsTestModule {
  static forRoot(options: SqsTestModuleOptions = {}): DynamicModule {
    const { sqs = new InMemorySqs({ autoCreateQueues: true }), ...moduleOptions } = options;
    return {
      module: SqsTestModule,
      global: moduleOptions.isGlobal ?? true,
      imports: [
        SqsModule.forRoot({
          ...moduleOptions,
          driverFactory: sqs.createDriverFactory(),
          clock: sqs.clock,
          shutdown: { abortReceive: true, ...moduleOptions.shutdown },
        }),
      ],
      providers: [{ provide: InMemorySqs, useValue: sqs }],
      exports: [InMemorySqs, SqsModule],
    };
  }
}
