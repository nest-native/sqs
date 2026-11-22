import { createParamDecorator, ExecutionContext } from '@nestjs/common';
import { SqsReceivedMessage } from './driver';
import { SqsContext } from './sqs-context';

/**
 * Parameter decorators for `@SqsHandler` methods, in the style of
 * `@nestjs/microservices`' `@Payload()` and `@Ctx()`. They are built with
 * Nest's public `createParamDecorator`, so param-level pipes declared with them
 * (`@SqsMessage(new ValidationPipe())`) run as they do on an HTTP argument.
 *
 * Handlers run under the `'rpc'` execution-context type: the decorators read
 * the parsed payload and the {@link SqsContext} through `switchToRpc()`.
 */

/**
 * Inject the message payload: the body parsed as JSON, or the body text when
 * it is not JSON. With a property name, inject that property of an object
 * payload, as `@Payload('id')` does.
 *
 * @example
 * ```ts
 * @SqsHandler('orders')
 * placed(@SqsMessage() order: OrderPlaced) {}
 * ```
 *
 * @publicApi
 */
export const SqsMessage = createParamDecorator(
  (property: string | undefined, ctx: ExecutionContext): unknown => {
    const payload = ctx.switchToRpc().getData<unknown>();
    if (property === undefined) {
      return payload;
    }
    return typeof payload === 'object' && payload !== null
      ? (payload as Record<string, unknown>)[property]
      : undefined;
  },
);

/**
 * Inject the message attributes, decoded (text for `String` and `Number`,
 * bytes for `Binary`); with a name, inject that one attribute.
 *
 * @example
 * ```ts
 * @SqsHandler('orders')
 * placed(@SqsMessage() order: OrderPlaced, @SqsAttributes('type') type: string) {}
 * ```
 *
 * @publicApi
 */
export const SqsAttributes = createParamDecorator(
  (name: string | undefined, ctx: ExecutionContext): unknown => {
    const attributes = ctx.switchToRpc().getContext<SqsContext>().getAttributes();
    return name === undefined ? attributes : attributes[name];
  },
);

/**
 * Inject the {@link SqsContext}: the queue, the raw message, the receive count,
 * the abort signal, and the manual `delete()` / `changeVisibility()` controls.
 *
 * @publicApi
 */
export const SqsCtx = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): SqsContext => ctx.switchToRpc().getContext<SqsContext>(),
);

/**
 * Inject the message exactly as SQS returned it — the SDK's `Message` shape.
 * A handler migrating from `@ssut/nestjs-sqs` keeps its signature with it:
 * `handle(@SqsRawMessage() message: Message)`.
 *
 * @publicApi
 */
export const SqsRawMessage = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): SqsReceivedMessage =>
    ctx.switchToRpc().getContext<SqsContext>().getMessage(),
);
