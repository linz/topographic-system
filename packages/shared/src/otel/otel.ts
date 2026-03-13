import { createHash } from 'node:crypto';

import type { Context, Span, Tracer } from '@opentelemetry/api';
import { trace as otelTrace } from '@opentelemetry/api';
import { propagation, context } from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { NodeSDK } from '@opentelemetry/sdk-node';
import {
  ATTR_SERVICE_INSTANCE_ID,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_NAMESPACE,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions';

import { CliId, CliInfo } from '../cli.info.ts';
import { logger } from '../log.ts';
import { instrumentFsa } from './instrument.fsa.ts';
import { instrumentZx } from './instrument.zx.ts';
let tracer: null | Tracer = null;
export function getTracer(): Tracer {
  if (tracer == null) tracer = otelTrace.getTracer('default');
  return tracer;
}

function readGithubEnv(): Record<string, string | undefined> | null {
  if (process.env['GITHUB_RUN_ID'] == null) return null;

  return {
    'github.run_id': process.env['GITHUB_RUN_ID'],
    'github.run_attempt': process.env['GITHUB_RUN_ATTEMPT'],
    'github.repository': process.env['GITHUB_REPOSITORY'],
    'github.workflow': process.env['GITHUB_WORKFLOW'],
  };
}

function maskKey(val: string): string {
  return createHash('sha256').update(val).digest('hex').slice(0, 12);
}

export function createOtelSdk(packageName: string): { sdk: NodeSDK; parentContext: Context } | null {
  const endPoint = process.env['OTEL_EXPORTER_OTLP_ENDPOINT'] ?? '';
  if (endPoint.trim() === '') return null;
  if (process.env['OTEL_SDK_DISABLED']) return null;

  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  const otelEnv = Object.keys(process.env).filter((f) => f.startsWith('OTEL_'));

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: packageName,
      [ATTR_SERVICE_NAMESPACE]: 'li-topo-maps',
      [ATTR_SERVICE_VERSION]: CliInfo.version,
      [ATTR_SERVICE_INSTANCE_ID]: CliId,

      ...readGithubEnv(),
    }),
    instrumentations: [],
  });

  instrumentFsa();
  instrumentZx();

  const parentContext = propagation.extract(context.active(), {
    traceparent: process.env['TRACEPARENT'],
  });

  const otel: Record<string, unknown> = {};
  for (const key of otelEnv) otel[key] = maskKey(process.env[key] ?? '');

  logger.info({ otel, traceParent: process.env['TRACEPARENT'] }, 'OpenTelemetry:Enabled');
  return { sdk, parentContext };
}

export async function trace<T>(name: string, fn: (span: Span) => Promise<T>, ctx?: Context): Promise<T> {
  const runner = async (span: Span) => {
    try {
      return await fn(span);
    } catch (error) {
      span.recordException(error as Error);
      span.setStatus({ code: 1, message: String(error) });
      throw error;
    } finally {
      span.end();
    }
  };
  if (ctx) return getTracer().startActiveSpan(name, {}, ctx, runner);
  return getTracer().startActiveSpan(name, runner);
}
