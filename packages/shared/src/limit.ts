import os from 'node:os';

import { number, option, optional } from 'cmd-ts';
import type { LimitFunction } from 'p-limit';
import pLimit from 'p-limit';

import { logger } from './log.ts';

export const qLimitDefault = 20;
export const workderLimitDefault = Math.max(1, Math.floor(os.cpus().length / 2));

export const concurrency = option({
  long: 'concurrency',
  description: 'Concurrency limit for parallel processing (default: 20)',
  type: optional(number),
  defaultValue: () => qLimitDefault,
  defaultValueIsSerializable: true,
});

export const worker = option({
  long: 'worker',
  description: 'Cpu workers limit for parallel processing (default: cpu cores / 2)',
  type: optional(number),
  defaultValue: () => workderLimitDefault,
  defaultValueIsSerializable: true,
});

export function qFromArgs(args: {} | { concurrency?: number; workder?: number }): LimitFunction {
  if ('concurrency' in args && typeof args.concurrency === 'number') return pLimit(args.concurrency);
  if ('workder' in args && typeof args.workder === 'number') return pLimit(args.workder);
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

/**
 * qMap and await all results.
 */
export async function qMapAll<T, R>(q: LimitFunction, arr: T[], fn: (item: T) => Promise<R>): Promise<R[]> {
  return Promise.all(qMap(q, arr, fn));
}
