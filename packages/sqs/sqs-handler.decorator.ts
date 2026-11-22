import { SetMetadata } from '@nestjs/common';
import { SQS_CONSUMER_METADATA, SQS_HANDLER_METADATA } from './constants';
import { SqsConsumerMetadata, SqsHandlerMetadata, SqsHandlerOptions } from './interfaces';

/**
 * Consume a queue with this method. Each message runs through the full Nest
 * enhancer pipeline (guards, interceptors, pipes, filters) before the method,
 * and is deleted when the method returns. A thrown error goes to the error
 * mapper: by default the message is retried after a backoff.
 *
 * `queue` is a logical key: configure its name or URL under
 * `SqsModule.forRoot({ queues })`, or use the queue's name directly. Omit it to
 * use the class's `@SqsConsumer(queue)`. A queue has exactly one handler.
 *
 * `options` are code-level defaults for the consumer; the queue's
 * configuration overrides them.
 *
 * @example
 * ```ts
 * @Injectable()
 * export class OrdersConsumer {
 *   @SqsHandler('orders')
 *   placed(@SqsMessage() order: OrderPlaced) {}
 * }
 * ```
 *
 * @publicApi
 */
export function SqsHandler(queue?: string, options: SqsHandlerOptions = {}): MethodDecorator {
  const metadata: SqsHandlerMetadata = { queue, options };
  return SetMetadata(SQS_HANDLER_METADATA, metadata);
}

/**
 * Give a class's `@SqsHandler` methods a default queue. Optional: handlers are
 * discovered on any provider or controller, with or without it.
 *
 * @publicApi
 */
export function SqsConsumer(queue?: string): ClassDecorator {
  const metadata: SqsConsumerMetadata = { queue };
  return SetMetadata(SQS_CONSUMER_METADATA, metadata);
}
