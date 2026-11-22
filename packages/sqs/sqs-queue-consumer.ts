import { LoggerService } from '@nestjs/common';
import { SqsClock } from './clock';
import { ResolvedConsumerOptions } from './sqs-consumer-options';
import { SqsDispatcher, SqsHandlerInvocation } from './sqs-dispatcher';
import { SqsErrorMapper } from './sqs-error-mapping';
import { SqsHeartbeat } from './sqs-heartbeat';
import { SqsPermits } from './sqs-permits';
import { SqsPoller } from './sqs-poller';
import { ResolvedSqsQueue } from './sqs-queue-registry';
import { SqsRetryBackoffOptions } from './sqs-retry-backoff';
import { SqsSettler } from './sqs-settler';

/** @internal */
export interface SqsQueueConsumerOptions {
  queue: ResolvedSqsQueue;
  consumer: ResolvedConsumerOptions;
  run(invocation: SqsHandlerInvocation): Promise<unknown>;
  errorMapper: SqsErrorMapper;
  retryBackoff?: Required<SqsRetryBackoffOptions>;
  clock: SqsClock;
  logger: LoggerService;
}

/**
 * One queue's consumer: a poller that receives, a dispatcher that runs the
 * handler, a heartbeat that keeps held messages invisible, and a settler that
 * deletes or changes visibility — sharing one set of permits, so the consumer
 * never holds more messages than its concurrency.
 *
 * @internal
 */
export class SqsQueueConsumer {
  private readonly settler: SqsSettler;
  private readonly dispatcher: SqsDispatcher;
  private readonly poller: SqsPoller;

  constructor(options: SqsQueueConsumerOptions) {
    const { queue, consumer, clock, logger } = options;
    const permits = new SqsPermits(consumer.concurrency);
    this.settler = new SqsSettler(queue.driver, queue.url, queue.key, logger);
    const heartbeat =
      consumer.heartbeat &&
      new SqsHeartbeat({ ...consumer.heartbeat, settler: this.settler, clock, logger, queueKey: queue.key });
    this.dispatcher = new SqsDispatcher({
      queue: {
        key: queue.key,
        name: queue.name,
        url: queue.url,
        isFifo: queue.isFifo,
        client: queue.client,
      },
      run: options.run,
      errorMapper: options.errorMapper,
      retryBackoff: options.retryBackoff,
      settler: this.settler,
      permits,
      clock,
      logger,
      heartbeat,
    });
    this.poller = new SqsPoller({
      queue,
      maxMessages: consumer.maxMessages,
      waitTimeSeconds: consumer.waitTimeSeconds,
      receiveTimeoutMs: consumer.receiveTimeoutMs,
      visibilityTimeout: consumer.visibilityTimeout,
      permits,
      clock,
      logger,
      dispatch: message => this.dispatcher.dispatch(message),
      release: messages => {
        for (const message of messages) {
          void this.settler.changeVisibility(message.ReceiptHandle as string, 0);
        }
      },
    });
  }

  start(): void {
    this.poller.start();
  }

  /**
   * Stop receiving, release the held messages that have not started (FIFO
   * messages waiting behind an earlier one of their group), drain what is
   * running (releasing whatever outlasts `drainTimeoutMs`), then send every
   * pending delete and visibility change.
   */
  async stop(options: { drainTimeoutMs: number; abortReceive: boolean }): Promise<void> {
    await this.poller.stop(options.abortReceive);
    await this.dispatcher.releaseWaiting();
    await this.dispatcher.drain(options.drainTimeoutMs);
    await this.settler.flush();
  }
}
