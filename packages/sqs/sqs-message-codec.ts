import { SqsMessageAttributeValue } from './driver';

/**
 * A decoded message attribute: the text of a `String` or `Number` attribute
 * (numbers stay text, so 38-digit values keep their precision) or the bytes of
 * a `Binary` one.
 */
export type SqsAttributeValue = string | Uint8Array;

/**
 * An attribute to send: text becomes `String`, a number `Number`, bytes
 * `Binary`, and an SDK-shaped `{ DataType, … }` value is sent as is (for
 * custom types such as `String.uuid`).
 */
export type SqsAttributeInput = string | number | Uint8Array | SqsMessageAttributeValue;

/**
 * The payload a handler receives: the body parsed as JSON when it parses, the
 * body text otherwise — the convention `@nest-native/kafka` uses. Reach the
 * exact text through `SqsContext.getBody()`.
 *
 * @internal
 */
export function parseSqsBody(body: string | undefined): unknown {
  if (body === undefined) {
    return undefined;
  }
  try {
    return JSON.parse(body);
  } catch {
    return body;
  }
}

/**
 * Decode received message attributes.
 *
 * @internal
 */
export function decodeSqsAttributes(
  attributes: Record<string, SqsMessageAttributeValue> | undefined,
): Record<string, SqsAttributeValue> {
  const decoded: Record<string, SqsAttributeValue> = {};
  for (const [name, attribute] of Object.entries(attributes ?? {})) {
    decoded[name] = attribute.DataType?.startsWith('Binary')
      ? (attribute.BinaryValue ?? new Uint8Array())
      : (attribute.StringValue ?? '');
  }
  return decoded;
}

/**
 * Encode attributes for sending. Throws a `TypeError` for a value that has no
 * SQS representation.
 *
 * @internal
 */
export function encodeSqsAttributes(
  attributes: Record<string, SqsAttributeInput>,
): Record<string, SqsMessageAttributeValue> {
  const encoded: Record<string, SqsMessageAttributeValue> = {};
  for (const [name, value] of Object.entries(attributes)) {
    encoded[name] = encodeAttribute(name, value);
  }
  return encoded;
}

function encodeAttribute(name: string, value: SqsAttributeInput): SqsMessageAttributeValue {
  if (typeof value === 'string') {
    return { DataType: 'String', StringValue: value };
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError(`The attribute "${name}" is not a finite number.`);
    }
    return { DataType: 'Number', StringValue: String(value) };
  }
  if (value instanceof Uint8Array) {
    return { DataType: 'Binary', BinaryValue: value };
  }
  if (typeof value === 'object' && value !== null && typeof value.DataType === 'string') {
    return value;
  }
  throw new TypeError(`The attribute "${name}" has no SQS representation.`);
}

/**
 * The body to send: a string as is, anything else as JSON. SQS bodies are
 * text, so bytes are refused rather than guessed at.
 *
 * @internal
 */
export function serializeSqsBody(body: unknown): string {
  if (typeof body === 'string') {
    return body;
  }
  if (body instanceof Uint8Array) {
    throw new TypeError('SQS message bodies are text: encode binary payloads (base64, for example) first.');
  }
  const json = body === undefined ? undefined : JSON.stringify(body);
  if (json === undefined) {
    throw new TypeError('The message body cannot be serialized to JSON.');
  }
  return json;
}
