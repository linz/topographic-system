import { createTypeSpecLibrary } from '@typespec/compiler';
import type { EmitContext, JSONSchemaType } from '@typespec/compiler';

import { emitJsonSchema } from './json-schema.ts';
import { emitParquetSchema } from './parquet.ts';
import { emitPydanticModels } from './pydantic.ts';
import { emitStacMetadata } from './stac.ts';
import { emitTypeScript } from './typescript.ts';

export interface EmitterOptions {
  'json-schema-output-dir'?: string;
  'typescript-output-file'?: string;
  'typescript-output-dir'?: string;
  'parquet-output-dir'?: string;
  'pydantic-output-file'?: string;
}

const EmitterOptionsSchema: JSONSchemaType<EmitterOptions> = {
  type: 'object',
  properties: {
    'json-schema-output-dir': { type: 'string', nullable: true },
    'typescript-output-file': { type: 'string', nullable: true },
    'typescript-output-dir': { type: 'string', nullable: true },
    'parquet-output-dir': { type: 'string', nullable: true },
    'pydantic-output-file': { type: 'string', nullable: true },
  },
  required: [],
};

export const $lib = createTypeSpecLibrary({
  name: '@linzjs/typespec-emitter',
  diagnostics: {},
  emitter: { options: EmitterOptionsSchema },
});

export async function $onEmit(context: EmitContext<EmitterOptions>) {
  const { options } = context;

  // 1. Emit JSON Schema if configured
  if (options['json-schema-output-dir']) {
    await emitJsonSchema(context, options['json-schema-output-dir']);
  }

  // 2. Emit TypeScript Types if configured
  const tsOutput = options['typescript-output-dir'] ?? options['typescript-output-file'];
  if (tsOutput) {
    await emitTypeScript(context, tsOutput);
  }

  // 3. Emit Parquet Schema if configured
  if (options['parquet-output-dir']) {
    await emitParquetSchema(context, options['parquet-output-dir']);
  }

  // 4. Emit Pydantic Models if configured
  if (options['pydantic-output-file']) {
    await emitPydanticModels(context, options['pydantic-output-file']);
  }

  // 5. Emit STAC metadata (collection.json per release and catalog.json)
  if (options['json-schema-output-dir']) {
    await emitStacMetadata(context, options['json-schema-output-dir']);
  }
}
