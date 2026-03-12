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
import { instrumentFsa } from './instrument.fsa.ts';

let tracer: null | Tracer = null;
export function getTracer(): Tracer {
  if (tracer == null) tracer = otelTrace.getTracer('default');
  return tracer;
}

export function createOtelSdk(packageName: string): NodeSDK | null {
  if (process.env['OTEL_EXPORTER_OTLP_ENDPOINT'] == null) return null;

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: packageName,
      [ATTR_SERVICE_NAMESPACE]: 'li-topo-maps',
      [ATTR_SERVICE_VERSION]: CliInfo.version,
      [ATTR_SERVICE_INSTANCE_ID]: CliId,
    }),
    instrumentations: [],
  });

  instrumentFsa();

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
