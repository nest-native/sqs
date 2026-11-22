import { DynamicModule, Module, Provider, Type } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { SqsModuleAsyncOptions, SqsModuleOptions } from './interfaces';
import { SqsClients } from './sqs-clients';
import { SqsConsumerExplorer } from './sqs-consumer.explorer';
import { SqsProducerService } from './sqs-producer.service';
import { SqsQueueRegistry } from './sqs-queue-registry';
import { SQS_CLIENT, SQS_MODULE_OPTIONS } from './tokens';

/**
 * The root module of `@nest-native/sqs`: the clients, the logical queues, the
 * producer, and the consumers of every `@SqsHandler` in the application.
 *
 * @publicApi
 */
@Module({})
export class SqsModule {
  /** Register the module with static options. */
  static forRoot(options: SqsModuleOptions = {}): DynamicModule {
    return {
      module: SqsModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule],
      providers: [{ provide: SQS_MODULE_OPTIONS, useValue: options }, ...coreProviders()],
      exports: exportedProviders(),
    };
  }

  /** Register the module with options resolved by a factory. */
  static forRootAsync(options: SqsModuleAsyncOptions): DynamicModule {
    return {
      module: SqsModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [
        ...(options.extraProviders ?? []),
        { provide: SQS_MODULE_OPTIONS, useFactory: options.useFactory, inject: options.inject ?? [] },
        ...coreProviders(),
      ],
      exports: exportedProviders(),
    };
  }

  /**
   * Register handler classes in a feature module. Handlers are discovered on
   * any provider or controller, so this is a convenience that also exports
   * them.
   */
  static forFeature(handlers: Type[] = []): DynamicModule {
    return { module: SqsModule, providers: [...handlers], exports: [...handlers] };
  }
}

function coreProviders(): Provider[] {
  return [
    {
      provide: SqsClients,
      useFactory: (options: SqsModuleOptions) => new SqsClients(options),
      inject: [SQS_MODULE_OPTIONS],
    },
    {
      provide: SQS_CLIENT,
      useFactory: (clients: SqsClients) => clients.get(),
      inject: [SqsClients],
    },
    {
      provide: SqsQueueRegistry,
      useFactory: (options: SqsModuleOptions, clients: SqsClients) =>
        new SqsQueueRegistry(options.queues ?? {}, clients),
      inject: [SQS_MODULE_OPTIONS, SqsClients],
    },
    SqsProducerService,
    SqsConsumerExplorer,
  ];
}

function exportedProviders(): Provider[] | DynamicModule['exports'] {
  return [SQS_MODULE_OPTIONS, SqsClients, SQS_CLIENT, SqsQueueRegistry, SqsProducerService];
}
