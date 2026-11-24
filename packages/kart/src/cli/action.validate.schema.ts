import { readFileSync } from 'fs';

import { fsa } from '@chunkd/fs';
import { concurrency, logger, qFromArgs, qMapAll, readParquetGroups, Url } from '@linzjs/topographic-system-shared';
import addFormats from 'ajv-formats';
import type { ErrorObject, SchemaObject } from 'ajv/dist/2020.js';
import Ajv from 'ajv/dist/2020.js';
import { command, flag, option, restPositionals } from 'cmd-ts';
import * as geojson from 'flatgeobuf/geojson';
import yaml from 'js-yaml';

async function loadSchema(schemaPath: URL): Promise<SchemaObject> {
  const content = await fsa.read(schemaPath);
  if (schemaPath.href.endsWith('.json')) return JSON.parse(String(content));
  if (schemaPath.href.endsWith('.yaml') || schemaPath.href.endsWith('.yml'))
    return yaml.load(String(content)) as SchemaObject;
  throw new Error(`Unsupported schema format for file ${schemaPath.href}`);
}

/**
 * Extract all property names from a JSON schema's properties object.
 * This handles nested property definitions at the top level.
 */
function getSchemaProperties(schema: SchemaObject): string[] {
  if (typeof schema['properties'] === 'object' && schema['properties'] !== null) {
    return Object.keys(schema['properties'] as Record<string, unknown>);
  }
  return [];
}

/**
 * Read FGB (FlatGeoBuf) file and yield records in groups.
 * FGB files contain geospatial features with properties that match schema fields.
 *
 * FlatGeoBuf has a known limitation where nullable columns are added to the file header
 * but their data is not properly serialized. To work around this, we fill in missing
 * properties with null values so they're treated as nullable by the schema validator.
 *
 * Note: We skip computed properties like 'bbox' which are derived from geometry.
 */
async function* readFgbGroups(
  path: URL,
  schemaProperties: string[] = [],
  batchSize: number = 1000,
): AsyncGenerator<unknown[]> {
  const filePath = new URL(path).pathname;
  const buffer = readFileSync(filePath);

  const batch: unknown[] = [];
  for await (const feature of geojson.deserialize(buffer)) {
    // Convert GeoJSON feature to record format: combine geometry and properties
    const record: any = { ...feature.properties, geometry: feature.geometry };

    // FlatGeoBuf may not deserialize nullable columns; fill in missing ones with null
    // so the validator can apply nullable schema rules.
    // Skip 'geometry' (already present) and 'bbox' (computed property, not data).
    const skipProps = new Set(['geometry', 'bbox']);
    for (const prop of schemaProperties) {
      if (!skipProps.has(prop) && !(prop in record)) {
        record[prop] = null;
      }
    }

    batch.push(record);

    if (batch.length >= batchSize) {
      yield batch.splice(0, batchSize);
    }
  }

  if (batch.length > 0) {
    yield batch;
  }
}

export const MaxErrorSamples = 5;
const MaxSampleLength = 80;
export interface ErrorAggregate {
  count: number;
  samples: string[];
}

// AJV keywords that only wrap nested branch errors; on their own they add no
// actionable detail (e.g. "must match a schema in anyOf").
const WrapperKeywords = new Set(['anyOf', 'oneOf', 'allOf', 'not', 'if']);

/**
 * Reduce the errors AJV reports for a single record to the actionable ones.
 *
 * With `allErrors`, a single bad value under a nullable enum (`anyOf: [enum, null]`)
 * produces three errors: the failed enum branch, the failed `null` branch, and the
 * `anyOf` wrapper. This drops the wrapper keywords, and where a property has more
 * than one remaining error it drops the `type: null` alternative, leaving just the
 * specific failure (e.g. the enum mismatch).
 */
export function collapseErrors(errors: ErrorObject[]): ErrorObject[] {
  const leaves = errors.filter((e) => !WrapperKeywords.has(e.keyword));

  const byPath = new Map<string, ErrorObject[]>();
  for (const e of leaves) {
    const list = byPath.get(e.instancePath) ?? [];
    list.push(e);
    byPath.set(e.instancePath, list);
  }

  const result: ErrorObject[] = [];
  for (const list of byPath.values()) {
    if (list.length <= 1) {
      result.push(...list);
      continue;
    }
    const specific = list.filter((e) => !(e.keyword === 'type' && e.params?.['type'] === 'null'));
    result.push(...(specific.length > 0 ? specific : list));
  }
  return result;
}

function formatSample(value: unknown): string {
  const str =
    typeof value === 'string'
      ? value
      : value == null || typeof value !== 'object'
        ? String(value)
        : JSON.stringify(value);
  return str.length > MaxSampleLength ? `${str.slice(0, MaxSampleLength)}…` : str;
}

/**
 * Pick a representative example for an error, or `undefined` when the error
 * message already carries the detail (e.g. a named missing property).
 */
export function errorSample(er: ErrorObject): string | undefined {
  const params = er.params as Record<string, unknown> | undefined;
  // The offending column name is the useful detail, not the whole record.
  if (typeof params?.['additionalProperty'] === 'string') return params['additionalProperty'];
  // "must have required property 'x'" already names the property.
  if (typeof params?.['missingProperty'] === 'string') return undefined;
  // For value-level failures (enum, type, pattern, ...) show the bad value itself.
  if (er.data == null || typeof er.data !== 'object') return formatSample(er.data);
  return undefined;
}

/**
 * Fold one record's (collapsed) errors into a running per-file summary, keeping
 * a count and a bounded set of distinct example values per distinct error.
 */
export function summariseErrors(errors: ErrorObject[], summary: Map<string, ErrorAggregate>): void {
  for (const er of collapseErrors(errors)) {
    const key = `${er.instancePath}:${er.message}`;
    let agg = summary.get(key);
    if (agg == null) {
      agg = { count: 0, samples: [] };
      summary.set(key, agg);
    }
    agg.count++;
    if (agg.samples.length < MaxErrorSamples) {
      const sample = errorSample(er);
      if (sample != null && !agg.samples.includes(sample)) agg.samples.push(sample);
    }
  }
}

export const ValidateSchemaCommand = command({
  name: 'validate-schema',
  description: 'Validate parquet or FGB files against a JSON schema',
  args: {
    concurrency,
    schema: option({ type: Url, long: 'schema', description: 'Path to JSON/YAML schema file' }),
    paths: restPositionals({
      type: Url,
      displayName: 'paths',
      description: 'Paths to parquet or FGB file(s) to validate against the schema',
    }),
    decodeGeometry: flag({
      long: 'decode-geometry',
      description: 'Whether to decode geometry columns',
      defaultValue: () => false,
      defaultValueIsSerializable: true,
    }),
  },
  async handler(args) {
    const q = qFromArgs(args);
    const ajv = new Ajv.default({ strict: true, allErrors: true, verbose: true });
    addFormats.default(ajv);

    const schemaContent = await loadSchema(args.schema);
    const validate = ajv.compile(schemaContent);
    await qMapAll(q, args.paths, async (path) => {
      const errorSummary = new Map<string, ErrorAggregate>();

      const startTime = performance.now();
      logger.info({ file: path.href, schema: args.schema.href }, 'ValidateSchema: Started');
      let valid = 0;
      let invalid = 0;

      // Auto-detect file format and use appropriate reader
      const fileExt = path.href.toLowerCase().split('.').pop();
      const isParquet = fileExt === 'parquet';
      const isFgb = fileExt === 'fgb';

      if (!isParquet && !isFgb) {
        throw new Error(`Unsupported file format: ${fileExt}. Expected 'parquet' or 'fgb'.`);
      }

      const reader = isParquet
        ? () => readParquetGroups(path, { decodeGeometry: args.decodeGeometry })
        : () => readFgbGroups(path, getSchemaProperties(schemaContent));

      for await (const group of reader()) {
        for (const record of group) {
          const ret = validate(record);
          if (ret) {
            valid++;
          } else {
            invalid++;
            summariseErrors(validate.errors ?? [], errorSummary);
          }
        }
      }

      for (const [error, { count, samples }] of errorSummary) {
        logger.error({ file: path.href, error, count, samples }, 'ValidateSchema:ErrorSummary');
      }

      let logOption = invalid > 0 ? ('error' as const) : ('info' as const);
      logger[logOption](
        {
          file: path.href,
          schema: args.schema.href,
          count: valid + invalid,
          invalid,
          duration: performance.now() - startTime,
        },
        'ValidateSchema:Done',
      );

      if (invalid > 0) throw new Error('Schema Validation failed for ' + path.href);
    });
  },
});
