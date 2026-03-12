import type { Span, Tracer } from '@opentelemetry/api';
import { trace as otelTrace } from '@opentelemetry/api';
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

export function createOtelSdk(packageName: string): NodeSDK | null {
  if (process.env['OTEL_EXPORTER_OTLP_ENDPOINT'] == null) return null;
  if (process.env['OTEL_SDK_DISABLED']) return null;

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

  logger.info({ otelEnv }, 'OpenTelemetry:Enabled');

  return sdk;
}

export async function trace<T>(name: string, fn: (span: Span) => Promise<T>): Promise<T> {
  return getTracer().startActiveSpan(name, async (span) => {
    try {
      return await fn(span);
    } catch (error) {
      span.recordException(error as Error);
      span.setStatus({ code: 1, message: String(error) });
      throw error;
    } finally {
      span.end();
    }
  });
}
