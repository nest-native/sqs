import { Inject } from '@nestjs/common';
import { SQS_CLIENT } from './tokens';

/**
 * Inject the default client's underlying `SQSClient`: the escape hatch for any
 * SQS operation the package does not wrap (`PurgeQueue`, queue attributes,
 * `StartMessageMoveTask`, …). Reach a named client with
 * `SqsClients.get(name)`.
 *
 * @example
 * ```ts
 * constructor(@InjectSqsClient() private readonly sqs: SQSClient) {}
 * ```
 *
 * @publicApi
 */
export const InjectSqsClient = (): ParameterDecorator & PropertyDecorator => Inject(SQS_CLIENT);
