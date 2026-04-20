import { number, option, optional } from 'cmd-ts';
import type { LimitFunction } from 'p-limit';
import pLimit from 'p-limit';

export const qLimitDefault = 10;
export function qFromArgs(args: {} | { concurrency?: number }): LimitFunction {
  if ('concurrency' in args && typeof args.concurrency === 'number') return pLimit(args.concurrency);
  return pLimit(qLimitDefault);
}

export const concurrency = option({
  long: 'concurrency',
  description: 'Concurrency limit for parallel processing (default: 10)',
  type: optional(number),
  defaultValue: () => qLimitDefault,
  defaultValueIsSerializable: true,
});
