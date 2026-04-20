import { number, option, optional } from 'cmd-ts';
import type { LimitFunction } from 'p-limit';
import pLimit from 'p-limit';

export const qLimitDefault = 4;
export function qFromArgs(args: {} | { concurrency?: number }): LimitFunction {
  if ('concurrency' in args && typeof args.concurrency === 'number') return pLimit(args.concurrency);
  return pLimit(qLimitDefault);
}

export const concurrency = option({
  long: 'concurrency',
  description: 'Concurrency limit for parallel processing (default: 4)',
  type: optional(number),
  defaultValue: () => qLimitDefault,
  defaultValueIsSerializable: true,
});
