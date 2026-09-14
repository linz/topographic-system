import { createTypeSpecLibrary } from '@typespec/compiler';
import type { EmitContext, JSONSchemaType, Model, Enum, Union } from '@typespec/compiler';

import { emitParquetSchema } from './parquet.ts';
import { emitPydanticModels } from './pydantic.ts';
import { emitTypeScript } from './typescript.ts';
import { collectTypes } from './utils.ts';

export interface EmitterOptions {
  'typescript-output-file'?: string;
  'parquet-output-dir'?: string;
  'pydantic-output-file'?: string;
}

const EmitterOptionsSchema: JSONSchemaType<EmitterOptions> = {
  type: 'object',
  properties: {
    'typescript-output-file': { type: 'string', nullable: true },
    'parquet-output-dir': { type: 'string', nullable: true },
    'pydantic-output-file': { type: 'string', nullable: true },
  },
  required: [],
};

export const $lib = createTypeSpecLibrary({
  name: '@linzjs/typespec-emitter',
  diagnostics: {},
  emitter: {
    options: EmitterOptionsSchema,
  },
});

export async function $onEmit(context: EmitContext<EmitterOptions>) {
  const { program, options } = context;

  const models = new Map<string, Model>();
  const enums = new Map<string, Enum>();
  const unions = new Map<string, Union>();

  collectTypes(program.getGlobalNamespaceType(), models, enums, unions);

  // 1. Emit TypeScript Types if configured
  if (options['typescript-output-file']) {
    const outputFile = options['typescript-output-file'];
    await emitTypeScript(program, outputFile, models, enums, unions);
  }

  // 2. Emit Parquet Schema if configured
  if (options['parquet-output-dir']) {
    const outputDir = options['parquet-output-dir'];
    await emitParquetSchema(program, outputDir, models);
  }

  // 3. Emit Pydantic Models if configured
  if (options['pydantic-output-file']) {
    const outputFile = options['pydantic-output-file'];
    await emitPydanticModels(program, outputFile, models);
  }
}
