import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Inject, Injectable, Module } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { InjectSqsClient } from '../inject-sqs-client.decorator';
import { SqsModuleOptions } from '../interfaces';
import { SqsClients } from '../sqs-clients';
import { SqsProducerService } from '../sqs-producer.service';
import { SqsModule } from '../sqs.module';
import { InMemorySqs } from '../testing/in-memory-sqs';
import { SqsTestModule } from '../testing/sqs-test.module';
import { SQS_CLIENT, SQS_MODULE_OPTIONS } from '../tokens';
import { captureNestLogs, createFakeDriver } from './helpers';

captureNestLogs();

@Injectable()
class UsesClient {
  constructor(@InjectSqsClient() readonly client: unknown) {}
}

describe('SqsModule.forRoot', () => {
  it('registers the options, the clients, the producer and the default client, globally by default', async () => {
    const driver = createFakeDriver({ client: { raw: 'client' } });
    const options: SqsModuleOptions = { driverFactory: () => driver };
    const definition = SqsModule.forRoot(options);
    assert.equal(definition.global, true);
    assert.equal(SqsModule.forRoot({ isGlobal: false }).global, false);
    assert.equal(SqsModule.forRoot().global, true);

    const moduleRef = await Test.createTestingModule({ imports: [definition], providers: [UsesClient] }).compile();
    assert.equal(moduleRef.get(SQS_MODULE_OPTIONS), options);
    assert.ok(moduleRef.get(SqsProducerService) instanceof SqsProducerService);
    assert.ok(moduleRef.get(SqsClients) instanceof SqsClients);
    assert.deepEqual(moduleRef.get(SQS_CLIENT), { raw: 'client' });
    assert.deepEqual(moduleRef.get(UsesClient).client, { raw: 'client' });
    await moduleRef.close();
    assert.equal(driver.destroyed, 1, 'the clients are released on shutdown');
  });
});

describe('SqsModule.forRootAsync', () => {
  it('resolves the options through a factory with its imports, injections and extra providers', async () => {
    const CONFIG = Symbol('CONFIG');

    @Module({ providers: [{ provide: CONFIG, useValue: 'https://q/123456789012/orders' }], exports: [CONFIG] })
    class ConfigModule {}

    const driver = createFakeDriver();
    const EXTRA = Symbol('EXTRA');
    const definition = SqsModule.forRootAsync({
      imports: [ConfigModule],
      inject: [CONFIG, EXTRA],
      extraProviders: [{ provide: EXTRA, useValue: 'extra' }],
      useFactory: (url: string, extra: string) => ({
        queues: { orders: url },
        driverFactory: () => createFakeDriver({ client: extra }),
      }),
    });
    assert.equal(definition.global, true);
    assert.equal(SqsModule.forRootAsync({ isGlobal: false, useFactory: () => ({}) }).global, false);

    const moduleRef = await Test.createTestingModule({ imports: [definition] }).compile();
    assert.equal(moduleRef.get(SQS_CLIENT), 'extra');
    const queue = await moduleRef.get(SqsProducerService).resolveQueue('orders');
    assert.equal(queue.url, 'https://q/123456789012/orders');
    await moduleRef.close();
    assert.equal(driver.destroyed, 0);

    const bare = await Test.createTestingModule({
      imports: [SqsModule.forRootAsync({ useFactory: () => ({ driverFactory: () => createFakeDriver() }) })],
    }).compile();
    await bare.close();
  });
});

describe('SqsModule.forFeature', () => {
  it('registers and exports the handler classes', () => {
    class Handlers {}
    assert.deepEqual(SqsModule.forFeature([Handlers]), { module: SqsModule, providers: [Handlers], exports: [Handlers] });
    assert.deepEqual(SqsModule.forFeature(), { module: SqsModule, providers: [], exports: [] });
  });
});

describe('SqsTestModule', () => {
  it('wires the module to an InMemorySqs that creates queues on demand', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [SqsTestModule.forRoot()] }).compile();
    await moduleRef.init();
    const sqs = moduleRef.get(InMemorySqs);
    await moduleRef.get(SqsProducerService).send('orders', { body: 'x' });
    assert.equal(sqs.messages('orders').length, 1);
    assert.equal(moduleRef.get(SQS_CLIENT), sqs);
    await moduleRef.close();
  });

  it('runs against a supplied InMemorySqs, on its clock, with overridable shutdown options', async () => {
    const sqs = new InMemorySqs();
    const definition = SqsTestModule.forRoot({ sqs, isGlobal: false, shutdown: { abortReceive: false } });
    assert.equal(definition.global, false);

    @Injectable()
    class ReadsOptions {
      constructor(@Inject(SQS_MODULE_OPTIONS) readonly options: SqsModuleOptions) {}
    }

    const moduleRef = await Test.createTestingModule({ imports: [definition], providers: [ReadsOptions] }).compile();
    const { options } = moduleRef.get(ReadsOptions);
    assert.equal(options.clock, sqs.clock);
    assert.deepEqual(options.shutdown, { abortReceive: false });
    assert.equal(moduleRef.get(InMemorySqs), sqs);
    await moduleRef.close();
  });
});
