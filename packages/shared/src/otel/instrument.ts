import type { Span } from '@opentelemetry/api';

import { trace } from './otel.ts';

export type InferArgs<T, K extends keyof T> = T[K] extends (...args: infer P) => unknown
  ? (...args: P) => string
  : never;

export type InferArgsWithSpan<T, K extends keyof T> = T[K] extends (...args: infer P) => any
  ? (instance: T, span: Span, ...args: P) => Promise<void> | void
  : never;

export type InferArgsWithSpanReturn<T, K extends keyof T> = T[K] extends (...args: infer P) => Promise<infer R>
  ? (instance: T, span: Span, value: Awaited<R>, ...args: P) => R
  : never;

interface MonitorOpts<T, K extends keyof T> {
  name: string | InferArgs<T, K>;

  before?: InferArgsWithSpan<T, K>;
  after?: InferArgsWithSpanReturn<T, K>;
}

export function monitor<T, K extends keyof T>(c: T, key: K, opts: MonitorOpts<T, K>): void {
  const oldHandler = c[key] as Function;
  if (oldHandler == null || typeof oldHandler !== 'function') {
    throw new Error(`${String(key)} does not exist on ${String(c)}`);
  }
  if (MonitorKey in oldHandler && oldHandler[MonitorKey] === MonitorSym) {
    throw new Error(`${String(key)} Already monitored`);
  }

  function newFn(...args: unknown[]) {
    const spanActual = typeof opts.name === 'function' ? opts.name(...args) : opts.name;
    return trace(spanActual, async (span) => {
      // @ts-ignore - typescript really doesnt like this `any`
      const ctx = this as any;
      try {
        if (opts.before) await opts.before(ctx, span, ...args);
        const result = await oldHandler.apply(ctx, args);
        if (opts.after) return opts.after(ctx, span, result, ...args);
        return result;
      } finally {
        span.end();
      }
    });
  }

  newFn[MonitorKey] = MonitorSym;
  c[key] = newFn as unknown as T[K];
}

/** Symbol to record if a function has been traced already */
export const MonitorSym = Symbol('trace:monitor');
export const MonitorKey = '_linz_trace';
