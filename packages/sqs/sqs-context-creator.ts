import { ContextType, ForbiddenException, PipeTransform } from '@nestjs/common';
import { RouteParamtypes } from '@nestjs/common/enums/route-paramtypes.enum';
import { ExecutionContextHost } from '@nestjs/core/helpers/execution-context-host';
import { STATIC_CONTEXT } from '@nestjs/core/injector/constants';
import { isObservable, lastValueFrom } from 'rxjs';
import { SqsHandlerInvocation } from './sqs-dispatcher';
import {
  Controller,
  SqsParamsPipes,
  SqsParamsResolver,
} from './sqs-params.resolver';

/**
 * Handlers run under Nest's `'rpc'` execution-context type, the type every
 * message transport uses, so guards, interceptors and filters that branch on it
 * (or call `host.switchToRpc()`) treat an SQS message as a message.
 */
const SQS_CONTEXT_TYPE: ContextType = 'rpc';

type SqsCallback = (...args: unknown[]) => unknown;
type SqsContextId = { id: number };

interface SqsGuardsContextCreatorLike {
  create(
    instance: Controller,
    callback: SqsCallback,
    moduleKey: string,
    contextId: SqsContextId,
    inquirerId?: string,
  ): unknown[];
}

interface SqsGuardsConsumerLike {
  tryActivate(
    guards: unknown[],
    args: unknown[],
    instance: Controller,
    callback: SqsCallback,
    type: ContextType,
  ): Promise<boolean>;
}

interface SqsInterceptorsContextCreatorLike {
  create(
    instance: Controller,
    callback: SqsCallback,
    moduleKey: string,
    contextId: SqsContextId,
    inquirerId?: string,
  ): unknown[];
}

interface SqsInterceptorsConsumerLike {
  intercept(
    interceptors: unknown[],
    args: unknown[],
    instance: Controller,
    callback: SqsCallback,
    next: () => Promise<unknown>,
    type: ContextType,
  ): Promise<unknown>;
}

interface SqsPipesContextCreatorLike {
  create(
    instance: Controller,
    callback: SqsCallback,
    moduleKey: string,
    contextId: SqsContextId,
    inquirerId?: string,
  ): PipeTransform[];
  createConcreteContext(
    metadata: unknown[],
    contextId?: SqsContextId,
    inquirerId?: string,
  ): PipeTransform[];
  setModuleContext(moduleKey: string): void;
}

interface SqsPipesConsumerLike {
  apply(
    value: unknown,
    metadata: { type: unknown; metatype?: unknown; data?: string },
    pipes: PipeTransform[],
  ): Promise<unknown>;
}

interface SqsExceptionHandlerLike {
  next(error: Error, executionContext: ExecutionContextHost): unknown;
}

interface SqsExceptionFiltersContextLike {
  create(
    instance: Controller,
    callback: SqsCallback,
    moduleKey: string,
    contextId: SqsContextId,
    inquirerId?: string,
  ): SqsExceptionHandlerLike;
}

/**
 * The Nest enhancer building blocks the context creator orchestrates. Built by
 * {@link createSqsEnhancerRuntime}.
 */
export interface SqsEnhancerRuntime {
  guardsContextCreator: SqsGuardsContextCreatorLike;
  guardsConsumer: SqsGuardsConsumerLike;
  interceptorsContextCreator: SqsInterceptorsContextCreatorLike;
  interceptorsConsumer: SqsInterceptorsConsumerLike;
  pipesContextCreator: SqsPipesContextCreatorLike;
  pipesConsumer: SqsPipesConsumerLike;
  exceptionFiltersContext: SqsExceptionFiltersContextLike;
}

/**
 * Everything the context creator needs to build one handler runner. Mirrors the
 * shape the explorer assembles per discovered `@SqsHandler`.
 */
export interface SqsHandlerContext {
  callback: SqsCallback;
  /**
   * The consumer class the handler lives on. Used to reflect the
   * parameter-decorator metadata once per handler.
   */
  metatype: Controller['constructor'];
  methodName: string;
  moduleKey: string;
  paramTypes: unknown[];
  inquirerId?: string;
  resolveContextId: () => SqsContextId;
  resolveInstance: (contextId: SqsContextId) => Promise<Controller>;
}

/**
 * Wraps `@SqsHandler` methods so each received message flows through the full
 * Nest enhancer pipeline — guards, interceptors, pipes and exception filters —
 * before the handler runs, as `@nestjs/microservices` does for its transports.
 * This is the piece that makes `@UseGuards`, `@UseInterceptors`, `@UsePipes`
 * and `@UseFilters` work on SQS handler methods.
 *
 * @internal
 */
export class SqsContextCreator {
  constructor(private readonly runtime: SqsEnhancerRuntime) {}

  /**
   * Build the per-message runner for a single handler. The returned function is
   * called once per consumed message; it resolves the (possibly request-scoped)
   * instance, runs the enhancer pipeline, and invokes the handler.
   */
  create(
    handler: SqsHandlerContext,
  ): (invocation: SqsHandlerInvocation) => Promise<unknown> {
    // Parameter-decorator metadata is static on the class, so reflect it once
    // when the handler is wired rather than per consumed message.
    const paramsResolver = this.createParamsResolver(handler);

    return async (invocation: SqsHandlerInvocation) => {
      const contextId = handler.resolveContextId();
      const instance = await handler.resolveInstance(contextId);
      const callback = this.resolveCallback(instance, handler);

      const contextArgs = [invocation.payload, invocation.context];
      const executionContext = this.createExecutionContext(
        instance,
        callback,
        contextArgs,
      );
      const exceptionHandler = this.runtime.exceptionFiltersContext.create(
        instance,
        callback,
        handler.moduleKey,
        contextId,
        handler.inquirerId,
      );

      try {
        return await this.run(handler, instance, callback, contextId, {
          executionContext,
          paramsResolver,
        });
      } catch (error) {
        return this.handleException(error, exceptionHandler, executionContext);
      }
    };
  }

  private createParamsResolver(
    handler: SqsHandlerContext,
  ): SqsParamsResolver {
    const pipesRuntime: SqsParamsPipes = {
      createConcrete: pipes =>
        this.runtime.pipesContextCreator.createConcreteContext(pipes),
      apply: (value, index, metatype, pipes) =>
        this.runtime.pipesConsumer.apply(
          value,
          {
            type: RouteParamtypes.BODY as unknown,
            metatype,
            data: undefined,
          },
          pipes,
        ),
    };
    return new SqsParamsResolver(
      handler.metatype,
      handler.methodName,
      handler.paramTypes,
      pipesRuntime,
    );
  }

  private resolveCallback(
    instance: Controller,
    handler: SqsHandlerContext,
  ): SqsCallback {
    const candidate = (instance as Record<string, unknown>)[handler.methodName];
    return typeof candidate === 'function'
      ? (candidate as SqsCallback)
      : handler.callback;
  }

  private createExecutionContext(
    instance: Controller,
    callback: SqsCallback,
    contextArgs: unknown[],
  ): ExecutionContextHost {
    const executionContext = new ExecutionContextHost(
      contextArgs,
      instance.constructor as never,
      callback as never,
    );
    executionContext.setType(SQS_CONTEXT_TYPE);
    return executionContext;
  }

  private async run(
    handler: SqsHandlerContext,
    instance: Controller,
    callback: SqsCallback,
    contextId: SqsContextId,
    state: {
      executionContext: ExecutionContextHost;
      paramsResolver: SqsParamsResolver;
    },
  ): Promise<unknown> {
    const contextArgs = state.executionContext.getArgs() as unknown[];
    const guards = this.runtime.guardsContextCreator.create(
      instance,
      callback,
      handler.moduleKey,
      contextId,
      handler.inquirerId,
    );
    await this.activateGuards(guards, contextArgs, instance, callback);

    const interceptors = this.runtime.interceptorsContextCreator.create(
      instance,
      callback,
      handler.moduleKey,
      contextId,
      handler.inquirerId,
    );
    const pipes = this.runtime.pipesContextCreator.create(
      instance,
      callback,
      handler.moduleKey,
      contextId,
      handler.inquirerId,
    );
    // Param-level pipes resolve against the owning module, mirroring how
    // `@nestjs/microservices`'s RpcContextCreator scopes custom-param pipes.
    this.runtime.pipesContextCreator.setModuleContext(handler.moduleKey);

    const invoke = this.createInvoker(handler, instance, callback, pipes, state);

    const result =
      interceptors.length === 0
        ? await invoke()
        : await this.runtime.interceptorsConsumer.intercept(
            interceptors,
            contextArgs,
            instance,
            callback,
            invoke,
            SQS_CONTEXT_TYPE,
          );
    return this.transformResult(result);
  }

  private async activateGuards(
    guards: unknown[],
    contextArgs: unknown[],
    instance: Controller,
    callback: SqsCallback,
  ): Promise<void> {
    if (guards.length === 0) {
      return;
    }

    const canActivate = await this.runtime.guardsConsumer.tryActivate(
      guards,
      contextArgs,
      instance,
      callback,
      SQS_CONTEXT_TYPE,
    );
    if (!canActivate) {
      throw new ForbiddenException();
    }
  }

  private createInvoker(
    handler: SqsHandlerContext,
    instance: Controller,
    callback: SqsCallback,
    pipes: PipeTransform[],
    state: {
      executionContext: ExecutionContextHost;
      paramsResolver: SqsParamsResolver;
    },
  ): () => Promise<unknown> {
    if (state.paramsResolver.hasParams()) {
      return this.createParamInvoker(handler, instance, callback, pipes, state);
    }
    return this.createPositionalInvoker(handler, instance, callback, pipes, state);
  }

  /**
   * Invoke a handler that declares parameter decorators. The args array is built
   * entirely from the `@SqsMessage()` / `@SqsAttributes()` / `@SqsCtx()`
   * factories and their param pipes; method-level pipes (`@UsePipes`) then run
   * over the resolved payload argument so both pipe tiers apply.
   */
  private createParamInvoker(
    handler: SqsHandlerContext,
    instance: Controller,
    callback: SqsCallback,
    pipes: PipeTransform[],
    state: {
      executionContext: ExecutionContextHost;
      paramsResolver: SqsParamsResolver;
    },
  ): () => Promise<unknown> {
    return async () => {
      const args = await state.paramsResolver.resolve(state.executionContext);
      if (pipes.length > 0) {
        args[0] = await this.applyPayloadPipes(args[0], pipes, handler.paramTypes);
      }
      return callback.apply(instance, args);
    };
  }

  /**
   * Invoke a handler that takes the positional `(payload, context)` arguments —
   * the convention for handlers written without parameter decorators. Method
   * pipes run over the payload, exactly as before parameter decorators existed.
   */
  private createPositionalInvoker(
    handler: SqsHandlerContext,
    instance: Controller,
    callback: SqsCallback,
    pipes: PipeTransform[],
    state: { executionContext: ExecutionContextHost },
  ): () => Promise<unknown> {
    return async () => {
      const args = [...(state.executionContext.getArgs() as unknown[])];
      if (pipes.length > 0) {
        args[0] = await this.applyPayloadPipes(args[0], pipes, handler.paramTypes);
      }
      return callback.apply(instance, args);
    };
  }

  private applyPayloadPipes(
    payload: unknown,
    pipes: PipeTransform[],
    paramTypes: unknown[],
  ): Promise<unknown> {
    return this.runtime.pipesConsumer.apply(
      payload,
      {
        type: RouteParamtypes.BODY as unknown,
        metatype: paramTypes[0],
        data: undefined,
      },
      pipes,
    );
  }

  private async handleException(
    error: unknown,
    exceptionHandler: SqsExceptionHandlerLike,
    executionContext: ExecutionContextHost,
  ): Promise<unknown> {
    const handled = await exceptionHandler.next(
      error as Error,
      executionContext,
    );
    return this.transformResult(handled);
  }

  private async transformResult(result: unknown): Promise<unknown> {
    if (isObservable(result)) {
      return lastValueFrom(result);
    }
    return result;
  }
}
