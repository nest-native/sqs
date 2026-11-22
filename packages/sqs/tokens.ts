/**
 * Injection tokens shared across the package.
 */

/** Token for the resolved {@link SqsModuleOptions}. */
export const SQS_MODULE_OPTIONS = Symbol('SQS_MODULE_OPTIONS');

/**
 * Token for the default client's underlying SQS client — the `SQSClient` the
 * module created, or the one you supplied. Inject it with
 * `@InjectSqsClient()`; reach named clients through `SqsClients.get(name)`.
 */
export const SQS_CLIENT = Symbol('SQS_CLIENT');
