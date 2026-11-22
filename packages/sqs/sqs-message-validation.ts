import { SQS_MAX_MESSAGE_ATTRIBUTES } from './constants';
import { SqsMessageAttributeValue, SqsSendEntry } from './driver';

/**
 * A message `SqsProducerService` refused before sending it, because SQS would
 * refuse it too. The message names the rule.
 *
 * @publicApi
 */
export class SqsMessageValidationError extends Error {
  override readonly name = 'SqsMessageValidationError';
}

/**
 * The characters SQS accepts in a body or a string attribute: `#x9 | #xA | #xD
 * | #x20–#xD7FF | #xE000–#xFFFD | #x10000–#x10FFFF`. With the `u` flag a lone
 * surrogate is its own code point, outside every range, so it is refused too.
 */
const INVALID_CHARACTER = /[^\t\n\r\u0020-\uD7FF\uE000-\uFFFD\u{10000}-\u{10FFFF}]/u;

/**
 * Whether `text` holds a character SQS refuses in a body or a string attribute.
 *
 * @internal
 */
export function containsInvalidSqsCharacter(text: string): boolean {
  return INVALID_CHARACTER.test(text);
}

/**
 * `MessageGroupId`, `MessageDeduplicationId` and `ReceiveRequestAttemptId`: 1
 * to 128 letters, digits and ASCII punctuation (printable ASCII but the space).
 */
const SQS_IDENTIFIER = /^[\x21-\x7E]{1,128}$/;

/**
 * Whether `value` is a valid `MessageGroupId`, `MessageDeduplicationId` or
 * `ReceiveRequestAttemptId`.
 *
 * @internal
 */
export function isSqsIdentifier(value: string): boolean {
  return SQS_IDENTIFIER.test(value);
}

/** Attribute names: letters, digits, `_`, `-` and `.`, up to 256. */
const ATTRIBUTE_NAME = /^[A-Za-z0-9_.-]{1,256}$/;

/** What a queue accepts, for {@link validateSqsSendEntry}. */
export interface SqsSendEntryRules {
  fifo: boolean;
  maxMessageBytes: number;
}

/**
 * Check an entry against the rules SQS enforces, so a doomed message fails
 * here, with the rule named, instead of as an `InvalidParameterValue`.
 *
 * @internal
 */
export function validateSqsSendEntry(entry: SqsSendEntry, rules: SqsSendEntryRules): void {
  validateBody(entry.body);
  validateAttributes(entry.messageAttributes ?? {});
  validateFifoFields(entry, rules.fifo);
  validateDelay(entry.delaySeconds, rules.fifo);
  const size = sqsMessageBytes(entry.body, entry.messageAttributes);
  if (size > rules.maxMessageBytes) {
    throw new SqsMessageValidationError(
      `The message is ${size} bytes, over the ${rules.maxMessageBytes}-byte limit (body plus attributes).`,
    );
  }
}

/**
 * The size SQS counts against its limit: the body, plus every attribute's
 * name, type and value, in UTF-8 bytes.
 *
 * @internal
 */
export function sqsMessageBytes(
  body: string,
  attributes: Record<string, SqsMessageAttributeValue> | undefined,
): number {
  let size = Buffer.byteLength(body, 'utf8');
  for (const [name, attribute] of Object.entries(attributes ?? {})) {
    size += Buffer.byteLength(name, 'utf8') + Buffer.byteLength(attribute.DataType ?? '', 'utf8');
    size += attribute.BinaryValue?.byteLength ?? Buffer.byteLength(attribute.StringValue ?? '', 'utf8');
  }
  return size;
}

function validateBody(body: string): void {
  if (body.length === 0) {
    throw new SqsMessageValidationError('The message body is empty; SQS requires at least one character.');
  }
  if (containsInvalidSqsCharacter(body)) {
    throw new SqsMessageValidationError(
      'The message body contains a character SQS refuses (allowed: #x9, #xA, #xD, #x20-#xD7FF, ' +
        '#xE000-#xFFFD, #x10000-#x10FFFF).',
    );
  }
}

function validateAttributes(attributes: Record<string, SqsMessageAttributeValue>): void {
  const names = Object.keys(attributes);
  if (names.length > SQS_MAX_MESSAGE_ATTRIBUTES) {
    throw new SqsMessageValidationError(
      `The message has ${names.length} attributes; SQS accepts at most ${SQS_MAX_MESSAGE_ATTRIBUTES}.`,
    );
  }
  for (const name of names) {
    if (!isValidAttributeName(name)) {
      throw new SqsMessageValidationError(`"${name}" is not a valid SQS attribute name.`);
    }
    const value = attributes[name].StringValue;
    if (value !== undefined && containsInvalidSqsCharacter(value)) {
      throw new SqsMessageValidationError(`The attribute "${name}" contains a character SQS refuses.`);
    }
  }
}

function isValidAttributeName(name: string): boolean {
  return (
    ATTRIBUTE_NAME.test(name) &&
    !name.startsWith('.') &&
    !name.endsWith('.') &&
    !name.includes('..') &&
    !/^(aws|amazon)\./i.test(name)
  );
}

function validateFifoFields(entry: SqsSendEntry, fifo: boolean): void {
  if (fifo && entry.messageGroupId === undefined) {
    throw new SqsMessageValidationError('A FIFO queue requires a groupId (MessageGroupId) on every message.');
  }
  if (!fifo && entry.messageDeduplicationId !== undefined) {
    throw new SqsMessageValidationError('deduplicationId applies only to FIFO queues.');
  }
  for (const [field, value] of [
    ['groupId', entry.messageGroupId],
    ['deduplicationId', entry.messageDeduplicationId],
  ] as const) {
    if (value !== undefined && !isSqsIdentifier(value)) {
      throw new SqsMessageValidationError(
        `${field} must be 1 to 128 characters of letters, digits and ASCII punctuation.`,
      );
    }
  }
}

function validateDelay(delaySeconds: number | undefined, fifo: boolean): void {
  if (delaySeconds === undefined) {
    return;
  }
  if (fifo) {
    throw new SqsMessageValidationError(
      'A FIFO queue does not accept a per-message delaySeconds; set DelaySeconds on the queue instead.',
    );
  }
  if (!Number.isInteger(delaySeconds) || delaySeconds < 0 || delaySeconds > 900) {
    throw new SqsMessageValidationError('delaySeconds must be an integer from 0 to 900.');
  }
}
