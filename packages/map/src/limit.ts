import type { LimitFunction } from 'p-limit';
import pLimit from 'p-limit';

export const qLimitDefault = 4;
export function qFromArgs(args: {} | { concurrency?: number }): LimitFunction {
  if ('concurrency' in args && typeof args.concurrency === 'number') return pLimit(args.concurrency);
  return pLimit(qLimitDefault);
}
