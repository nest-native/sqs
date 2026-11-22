/**
 * Reflection metadata keys shared by the decorators and the explorer that
 * discovers them. They are string keys so they survive the `reflect-metadata`
 * round trip across compilation units.
 */

/** Marks a method as an SQS handler. Carries the {@link SqsHandlerMetadata}. */
export const SQS_HANDLER_METADATA = 'nest-native:sqs:handler';

/** Marks a class with a default queue. Carries the {@link SqsConsumerMetadata}. */
export const SQS_CONSUMER_METADATA = 'nest-native:sqs:consumer';

/**
 * The longest visibility timeout SQS accepts, and the budget a received message
 * has in total: "the visibility timeout has a maximum limit of 12 hours from
 * when the message is first received. Extending the timeout doesn't reset this
 * 12-hour limit."
 */
export const SQS_MAX_VISIBILITY_SECONDS = 43_200;

/** The largest message, and the largest batch payload, SQS accepts: 1 MiB. */
export const SQS_MAX_MESSAGE_BYTES = 1_048_576;

/** The most entries one send, delete or visibility batch may carry. */
export const SQS_MAX_BATCH_ENTRIES = 10;

/** The most message attributes one message may carry. */
export const SQS_MAX_MESSAGE_ATTRIBUTES = 10;

/** The longest `WaitTimeSeconds` a receive may long-poll for. */
export const SQS_MAX_WAIT_TIME_SECONDS = 20;
