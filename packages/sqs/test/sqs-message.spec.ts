import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  decodeSqsAttributes,
  encodeSqsAttributes,
  parseSqsBody,
  serializeSqsBody,
  SqsAttributeInput,
} from '../sqs-message-codec';
import {
  containsInvalidSqsCharacter,
  sqsMessageBytes,
  SqsMessageValidationError,
  validateSqsSendEntry,
} from '../sqs-message-validation';
import { SqsSendEntry } from '../driver';

const STANDARD = { fifo: false, maxMessageBytes: 1_048_576 };
const FIFO = { fifo: true, maxMessageBytes: 1_048_576 };

function entry(overrides: Partial<SqsSendEntry> = {}): SqsSendEntry {
  return { id: '0', body: 'hello', ...overrides };
}

function rejects(candidate: SqsSendEntry, rules: typeof STANDARD, pattern: RegExp): void {
  assert.throws(
    () => validateSqsSendEntry(candidate, rules),
    (error: unknown) => error instanceof SqsMessageValidationError && pattern.test(error.message),
  );
}

describe('parseSqsBody', () => {
  it('parses JSON, keeps other text, and passes undefined through', () => {
    assert.deepEqual(parseSqsBody('{"id":1}'), { id: 1 });
    assert.equal(parseSqsBody('42'), 42);
    assert.equal(parseSqsBody('not json'), 'not json');
    assert.equal(parseSqsBody(undefined), undefined);
  });
});

describe('decodeSqsAttributes', () => {
  it('decodes text and bytes, by data type', () => {
    const bytes = new Uint8Array([1, 2]);
    assert.deepEqual(
      decodeSqsAttributes({
        type: { DataType: 'String', StringValue: 'OrderPlaced' },
        count: { DataType: 'Number', StringValue: '12345678901234567890' },
        custom: { DataType: 'String.uuid', StringValue: 'abc' },
        blob: { DataType: 'Binary', BinaryValue: bytes },
        empty: { DataType: 'Binary' },
        missing: { DataType: 'String' },
        untyped: { StringValue: 'x' },
      }),
      {
        type: 'OrderPlaced',
        count: '12345678901234567890',
        custom: 'abc',
        blob: bytes,
        empty: new Uint8Array(),
        missing: '',
        untyped: 'x',
      },
    );
    assert.deepEqual(decodeSqsAttributes(undefined), {});
  });
});

describe('encodeSqsAttributes', () => {
  it('encodes text, numbers, bytes and SDK-shaped values', () => {
    const bytes = new Uint8Array([7]);
    assert.deepEqual(
      encodeSqsAttributes({
        type: 'OrderPlaced',
        version: 2,
        blob: bytes,
        id: { DataType: 'String.uuid', StringValue: 'abc' },
      }),
      {
        type: { DataType: 'String', StringValue: 'OrderPlaced' },
        version: { DataType: 'Number', StringValue: '2' },
        blob: { DataType: 'Binary', BinaryValue: bytes },
        id: { DataType: 'String.uuid', StringValue: 'abc' },
      },
    );
  });

  it('refuses values SQS cannot represent', () => {
    assert.throws(() => encodeSqsAttributes({ n: Number.NaN }), /not a finite number/);
    for (const value of [null, true, { StringValue: 'no type' }] as unknown as SqsAttributeInput[]) {
      assert.throws(() => encodeSqsAttributes({ bad: value }), /no SQS representation/);
    }
  });
});

describe('serializeSqsBody', () => {
  it('sends strings as is and everything else as JSON', () => {
    assert.equal(serializeSqsBody('raw'), 'raw');
    assert.equal(serializeSqsBody({ id: 1 }), '{"id":1}');
    assert.equal(serializeSqsBody(7), '7');
  });

  it('refuses bytes, undefined and values JSON cannot represent', () => {
    assert.throws(() => serializeSqsBody(new Uint8Array([1])), /bodies are text/);
    assert.throws(() => serializeSqsBody(undefined), /cannot be serialized/);
    assert.throws(() => serializeSqsBody(() => 1), /cannot be serialized/);
  });
});

describe('validateSqsSendEntry', () => {
  it('accepts a plain message, and a FIFO one with its group', () => {
    validateSqsSendEntry(entry({ delaySeconds: 900, messageAttributes: { a: { DataType: 'String', StringValue: 'b' } } }), STANDARD);
    validateSqsSendEntry(entry({ messageGroupId: 'tenant-1', messageDeduplicationId: 'm-1' }), FIFO);
    validateSqsSendEntry(entry({ messageGroupId: 'tenant-1' }), STANDARD);
  });

  it('refuses an empty body and characters SQS rejects', () => {
    rejects(entry({ body: '' }), STANDARD, /empty/);
    rejects(entry({ body: `a${String.fromCharCode(0)}b` }), STANDARD, /character SQS refuses/);
    rejects(entry({ body: String.fromCharCode(0xd800) }), STANDARD, /character SQS refuses/);
    rejects(entry({ body: String.fromCharCode(0xfffe) }), STANDARD, /character SQS refuses/);
    rejects(
      entry({ messageAttributes: { a: { DataType: 'String', StringValue: String.fromCharCode(1) } } }),
      STANDARD,
      /attribute "a" contains/,
    );
  });

  it('refuses more than 10 attributes and invalid attribute names', () => {
    const many = Object.fromEntries(
      Array.from({ length: 11 }, (_, index) => [`a${index}`, { DataType: 'String', StringValue: 'x' }]),
    );
    rejects(entry({ messageAttributes: many }), STANDARD, /11 attributes/);
    for (const name of ['.lead', 'trail.', 'a..b', 'AWS.reserved', 'amazon.x', 'bad name', 'x'.repeat(257)]) {
      rejects(entry({ messageAttributes: { [name]: { DataType: 'String', StringValue: 'x' } } }), STANDARD, /attribute name/);
    }
  });

  it('enforces the FIFO fields', () => {
    rejects(entry(), FIFO, /requires a groupId/);
    rejects(entry({ messageDeduplicationId: 'd' }), STANDARD, /only to FIFO/);
    rejects(entry({ messageGroupId: '' }), FIFO, /groupId must be/);
    rejects(entry({ messageGroupId: 'g', messageDeduplicationId: 'x'.repeat(129) }), FIFO, /deduplicationId must be/);
    rejects(entry({ messageGroupId: 'has space' }), STANDARD, /groupId must be/);
  });

  it('enforces delaySeconds', () => {
    rejects(entry({ messageGroupId: 'g', delaySeconds: 0 }), FIFO, /does not accept a per-message delaySeconds/);
    for (const delaySeconds of [-1, 901, 1.5]) {
      rejects(entry({ delaySeconds }), STANDARD, /delaySeconds must be/);
    }
  });

  it('enforces the size limit on the body plus the attributes', () => {
    rejects(entry({ body: 'x'.repeat(11) }), { fifo: false, maxMessageBytes: 10 }, /11 bytes, over the 10-byte limit/);
    validateSqsSendEntry(entry({ body: 'x'.repeat(10) }), { fifo: false, maxMessageBytes: 10 });
  });
});

describe('sqsMessageBytes', () => {
  it('counts the body and every attribute name, type and value in UTF-8 bytes', () => {
    assert.equal(sqsMessageBytes('é', undefined), 2);
    assert.equal(
      sqsMessageBytes('ab', {
        n: { DataType: 'String', StringValue: 'xyz' },
        b: { DataType: 'Binary', BinaryValue: new Uint8Array(4) },
        u: {},
      }),
      2 + (1 + 6 + 3) + (1 + 6 + 4) + 1,
    );
  });

  it('flags characters outside the allowed set', () => {
    assert.equal(containsInvalidSqsCharacter('tab\tnewline\ncr\r ok'), false);
    assert.equal(containsInvalidSqsCharacter(String.fromCodePoint(0x1f600)), false);
    assert.equal(containsInvalidSqsCharacter(String.fromCharCode(0x0b)), true);
  });
});
