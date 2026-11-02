import { createAssetEmitter } from '@typespec/asset-emitter';
import type { EmittedSourceFile, SourceFile } from '@typespec/asset-emitter';
import type { EmitContext, Type } from '@typespec/compiler';
import type { JsonSchemaEmitterOutputOptions } from '@typespec/json-schema';
import { JsonSchemaEmitter } from '@typespec/json-schema';

import type { EmitterOptions } from './index.ts';
import type { EmissionUnit } from './plan.ts';
import { unitOutputDir } from './plan.ts';
import { visitIdentifiedTypes, visitOwnIdentifiedTypes } from './utils.ts';

class VersionedJsonSchemaEmitter extends JsonSchemaEmitter {
  override sourceFile(sourceFile: SourceFile<Record<string, unknown>>): EmittedSourceFile {
    const result = super.sourceFile(sourceFile);
    try {
      const versionStr = this.emitter.getOptions().versionStr ?? '';
      const json = JSON.parse(result.contents);
      if (json.$id) {
        json.$id = `${json.$id}_v${versionStr}`;
      }
      return { contents: JSON.stringify(json, null, 2), path: result.path };
    } catch (error) {
      this.emitter
        .getProgram()
        .trace('typespec-emitter.json-schema', `Failed to version $id for ${result.path}: ${String(error)}`);
      return result;
    }
  }
}

async function writeJsonSchema(
  context: EmitContext<EmitterOptions>,
  types: Type[],
  outputDir: string,
  versionStr?: string,
): Promise<void> {
  const { program } = context;
  const emitContext: EmitContext<JsonSchemaEmitterOutputOptions> = {
    ...context,
    emitterOutputDir: outputDir,
    options: { ...context.options, 'file-type': 'json', ...(versionStr != null ? { versionStr } : {}) },
  };

  const emitter = createAssetEmitter(
    program,
    versionStr != null ? VersionedJsonSchemaEmitter : JsonSchemaEmitter,
    emitContext,
  );

  for (const item of types) {
    emitter.emitType(item);
  }

  await emitter.writeOutput();
}

/**
 * Emits JSON Schema for every `@jsonSchema`-tagged type, one output per (version × product) unit
 * — see `planEmission`. Each unit combines its own namespace's types with any declared directly
 * on the shared root at that version.
 */
export async function emitJsonSchema(
  context: EmitContext<EmitterOptions>,
  units: EmissionUnit[],
  outputDir: string,
): Promise<void> {
  const { program } = context;

  for (const unit of units) {
    const types: Type[] = [];
    const visit = (_id: string, type: Type): void => {
      types.push(type);
    };
    if (unit.sharedNs) visitOwnIdentifiedTypes(program, unit.sharedNs, visit);
    if (unit.ownOnly) {
      visitOwnIdentifiedTypes(program, unit.ns, visit);
    } else {
      visitIdentifiedTypes(program, unit.ns, visit);
    }

    await writeJsonSchema(context, types, unitOutputDir(outputDir, unit.path), unit.versionStr);
  }
}
