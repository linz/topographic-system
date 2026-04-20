import { number, option, optional } from 'cmd-ts';
import type { LimitFunction } from 'p-limit';
import pLimit from 'p-limit';

import { logger } from './log.ts';

export const qLimitDefault = 10;

export const concurrency = option({
  long: 'concurrency',
  description: 'Concurrency limit for parallel processing (default: 10)',
  type: optional(number),
  defaultValue: () => qLimitDefault,
  defaultValueIsSerializable: true,
});

export function qFromArgs(args: {} | { concurrency?: number }): LimitFunction {
  if ('concurrency' in args && typeof args.concurrency === 'number') return pLimit(args.concurrency);
  return pLimit(qLimitDefault);
}

/**
 * Maps over an array todos with a concurrency limit using a LimitFunction (from p-limit).
 */
export function qMap<T, R>(q: LimitFunction, arr: T[], fn: (item: T) => Promise<R>): Promise<R>[] {
  return arr.map((item) => {
    return q(() => fn(item)).catch((err: unknown) => {
      logger.fatal({ err }, 'Error');
      throw err;
    });
  });
}
