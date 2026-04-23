import { zstdDecompressSync } from 'zlib';

import { fsa } from '@chunkd/fs';
import { concurrency, logger, qFromArgs, qMapAll, Url } from '@linzjs/topographic-system-shared';
import type { SchemaObject } from 'ajv/dist/2020.js';
import Ajv from 'ajv/dist/2020.js';
import { command, option, restPositionals } from 'cmd-ts';
import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet';
import yaml from 'js-yaml';

async function loadSchema(schemaPath: URL): Promise<SchemaObject> {
  const content = await fsa.read(schemaPath);
  if (schemaPath.href.endsWith('.json')) return JSON.parse(String(content));
  if (schemaPath.href.endsWith('.yaml') || schemaPath.href.endsWith('.yml'))
    return yaml.load(String(content)) as SchemaObject;
  throw new Error(`Unsupported schema format for file ${schemaPath.href}`);
}

export const ValidateSchemaCommand = command({
  name: 'validate-schema',
  description: 'Validate that a parquet file matches a JSON schema',
  args: {
    concurrency,
    schema: option({ type: Url, long: 'schema', description: 'Path to YAML schema file emitted by typespec' }),
    paths: restPositionals({
      type: Url,
      displayName: 'paths',
      description: 'Paths to parquet file and YAML schema file',
    }),
  },
  async handler(args) {
    const q = qFromArgs(args);
    const ajv = new Ajv.default({ strict: false, allErrors: true });

    const schemaContent = await loadSchema(args.schema);
    const validate = ajv.compile(schemaContent);

    await qMapAll(q, args.paths, async (path) => {
      const source = fsa.source(path);
      const head = await fsa.head(path);
      if (head == null) throw new Error('Missing file: ' + path.href);
      const asyncBuffer = {
        byteLength: head.size as number,
        slice(start: number, end?: number) {
          return source.fetch(start, end == null ? undefined : end - start);
        },
      };
      const metadata = await parquetMetadataAsync(asyncBuffer);

      const startTime = performance.now();
      const ZSTD = (input: Uint8Array): Uint8Array => zstdDecompressSync(input);

      logger.info({ file: path.href, schema: args.schema.href }, 'Validate: Started');
      let rowStart = 0;
      let valid = 0;
      let invalid = 0;
      for (const group of metadata.row_groups) {
        const rowEnd = rowStart + Number(group.num_rows);
        const groupData = await parquetReadObjects({
          file: asyncBuffer,
          metadata,
          compressors: { ZSTD },
          rowStart,
          rowEnd,
        });

        const errorSummary = new Map<string, number>();

        for (const record of groupData) {
          const ret = validate(record);
          if (ret) {
            valid++;
          } else {
            invalid++;
            for (const er of validate.errors ?? []) {
              const key = `${er.instancePath}:${er.message}`;
              errorSummary.set(key, (errorSummary.get(key) ?? 0) + 1);
            }
          }
        }
        rowStart = rowEnd;

        for (const [error, count] of errorSummary) {
          logger.error({ file: path.href, error, count }, 'Validate:ErrorSummary');
        }
      }

      logger.info(
        {
          file: path.href,
          schema: args.schema.href,
          count: valid + invalid,
          invalid,
          duration: performance.now() - startTime,
        },
        'Validate:Done',
      );
    });
  },
});
