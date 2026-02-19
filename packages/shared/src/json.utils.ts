import { logger } from './log.ts';

const BigIntFields = new Set([
  'table:row_count',
  'null_count',
  'distinct_count',
  'max',
  'min',
  'max_value',
  'min_value',
]);

export function serializeBigInt(key: string, value: unknown): unknown {
  if (typeof value === 'bigint') {
    if (!BigIntFields.has(key)) {
      logger.warn({ key, value: value.toString() }, 'JsonUtils:BigIntFieldNotInPredefinedList');
    }
    if (value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)) {
      return Number(value);
    }
    return value.toString();
  }
  return value;
}

export function deserializeBigInt(key: string, value: unknown): unknown {
  // Fixme: this will convert any string that looks like a number to a BigInt, which may not be desirable.
  //  Consider additional checks or adding a suffix to identify BigInt strings.
  const isNumberString = (v: string): boolean => /^-?\d+$/.test(v);
  if (BigIntFields.has(key) && typeof value === 'string' && isNumberString(value)) {
    return BigInt(value);
  }
  return value;
}
