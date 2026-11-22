import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { ArgumentsHost, Catch, ExceptionFilter, Injectable, Logger, UseFilters } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { SqsContext } from '../sqs-context';
import { toDeadLetterMessage } from '../sqs-dead-letter';
import { SqsHandler } from '../sqs-handler.decorator';
import { SqsProducerService } from '../sqs-producer.service';
import { InMemorySqs } from '../testing/in-memory-sqs';
import { SqsTestModule } from '../testing/sqs-test.module';
import { createRecordingLogger } from './helpers';

/** A failure that no retry can fix. */
class PoisonMessageError extends Error {}

/**
 * The documented "dead-letter now" recipe: dead letters are the queue's redrive
 * policy, and a failure that is certain does not have to wait for
 * `maxReceiveCount` receives. An exception filter — DI-enabled, and awaited
 * before the message is settled — sends a copy to the dead-letter queue with
 * the producer and returns, which settles the message as handled: deleted.
 */
@Catch(PoisonMessageError)
@Injectable()
class DeadLetterFilter implements ExceptionFilter {
  constructor(private readonly producer: SqsProducerService) {}

  async catch(_error: PoisonMessageError, host: ArgumentsHost): Promise<void> {
    const context = host.switchToRpc().getContext<SqsContext>();
    await this.producer.send('orders-dlq', toDeadLetterMessage(context));
  }
}

@Injectable()
class OrdersConsumer {
  @SqsHandler('orders')
  @UseFilters(DeadLetterFilter)
  placed(): void {
    throw new PoisonMessageError('unknown order type');
  }
}

describe('the dead-letter-now recipe', () => {
  it('moves a certain failure to the dead-letter queue at once, body and attributes intact', async () => {
    Logger.overrideLogger(createRecordingLogger());
    const sqs = new InMemorySqs();
    sqs.createQueue('orders-dlq');
    sqs.createQueue('orders', {
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: sqs.queueArn('orders-dlq'), maxReceiveCount: 5 }),
    });
    const moduleRef = await Test.createTestingModule({
      imports: [SqsTestModule.forRoot({ sqs })],
      providers: [OrdersConsumer, DeadLetterFilter],
    }).compile();
    await moduleRef.init();

    sqs.sendMessage('orders', '{"type":"unknown"}', { attributes: { tenant: 'acme' } });
    await sqs.idle();

    assert.deepEqual(sqs.messages('orders'), [], 'deleted from its queue after one attempt');
    const [dead] = sqs.messages('orders-dlq');
    assert.equal(dead.body, '{"type":"unknown"}');
    assert.deepEqual(dead.attributes, { tenant: { DataType: 'String', StringValue: 'acme' } });
    await moduleRef.close();
  });
});
