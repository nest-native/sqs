import { SqsContext } from './sqs-context';
import { SqsOutgoingMessage } from './sqs-producer.service';

/**
 * The message to send to a dead-letter queue yourself: the received body,
 * byte for byte, with its message attributes.
 *
 * Dead letters are the queue's redrive policy: SQS moves a message that keeps
 * failing, atomically, after `maxReceiveCount` receives. Use this primitive
 * when a failure is certain and you do not want to wait that many attempts —
 * in an exception filter, send it with `SqsProducerService`, and the message is
 * then deleted from its queue because the filter handled the error. Unlike a
 * redrive move, a message sent this way records no source queue, so redriving
 * it back is a move you name the destination of.
 *
 * @publicApi
 */
export function toDeadLetterMessage(context: SqsContext): SqsOutgoingMessage<string> {
  const attributes = context.getMessageAttributes();
  return Object.keys(attributes).length === 0
    ? { body: context.getBody() }
    : { body: context.getBody(), attributes: { ...attributes } };
}
