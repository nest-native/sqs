/**
 * Testing utilities for `@nest-native/sqs`: an in-memory SQS with real
 * visibility semantics, the virtual clock it runs on, and a module that wires
 * `SqsModule` to it. Import them from `@nest-native/sqs/testing`, so test
 * scaffolding never enters an application's production imports.
 */
export * from './in-memory-sqs';
export * from './virtual-clock';
export * from './sqs-test.module';
