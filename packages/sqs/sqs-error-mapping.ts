import { SqsContext } from './sqs-context';

/**
 * What the transport does with a message whose handler (or enhancer pipeline)
 * threw an error that no exception filter handled.
 *
 * - `'retry'` — leave the message on the queue and make it visible again after
 *   the retry backoff. SQS counts the receive, so a message that keeps failing
 *   reaches the queue's dead-letter queue after `maxReceiveCount` receives.
 * - `'delete'` — acknowledge and drop the message. Use it for messages you have
 *   decided to discard; to keep one, send it somewhere first (see the
 *   dead-letter recipe in the README).
 */
export type SqsErrorBehavior = 'retry' | 'delete';

/**
 * Decides the {@link SqsErrorBehavior} of a failed message. It may be async: the
 * transport awaits it before settling the message. A mapper that throws, or
 * returns anything but `'delete'`, retries.
 */
export type SqsErrorMapper = (
  error: unknown,
  context: SqsContext,
) => SqsErrorBehavior | Promise<SqsErrorBehavior>;

/**
 * Retry every error. SQS has no partition to unblock, so a failing message
 * blocks nobody while it waits; the queue's redrive policy is what moves a
 * message that never succeeds to the dead-letter queue, where it is kept.
 */
export const defaultSqsErrorMapper: SqsErrorMapper = () => 'retry';
