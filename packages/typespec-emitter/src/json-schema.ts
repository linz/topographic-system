import { createAssetEmitter } from '@typespec/asset-emitter';
import type { EmitContext, Type } from '@typespec/compiler';
import { JsonSchemaEmitter, getId } from '@typespec/json-schema';

import { planEmission, unitOutputDir } from './plan.ts';
import { getChildNamespaces } from './utils.ts';

class VersionedJsonSchemaEmitter extends JsonSchemaEmitter {
  override sourceFile(sourceFile: any) {
    const result = super.sourceFile(sourceFile);
    try {
      const versionStr = (this.emitter.getOptions() as any).versionStr ?? '';
      const json = JSON.parse(result.contents);
      if (json.$id) {
        json.$id = `${json.$id}_v${versionStr}`;
      }
      return { contents: JSON.stringify(json, null, 2), path: result.path };
    } catch {
      return result;
    }
  }
}

/** Collects every model/union/enum/scalar with a `@jsonSchema` id declared directly on `ns`. */
function collectOwnTypes(program: any, ns: any, out: Type[]): void {
  if ('models' in ns && ns.models) {
    for (const m of ns.models.values()) if (getId(program, m)) out.push(m);
  }
  if ('unions' in ns && ns.unions) {
    for (const u of ns.unions.values()) if (getId(program, u)) out.push(u);
  }
  if ('enums' in ns && ns.enums) {
    for (const e of ns.enums.values()) if (getId(program, e)) out.push(e);
  }
  if ('scalars' in ns && ns.scalars) {
    for (const s of ns.scalars.values()) if (getId(program, s)) out.push(s);
  }
}

/** Collects every model/union/enum/scalar with a `@jsonSchema` id, recursing into sub-namespaces. */
function collectIdentifiedTypes(program: any, ns: any, out: Type[]): void {
  collectOwnTypes(program, ns, out);
  for (const { ns: sub } of getChildNamespaces(ns)) {
    collectIdentifiedTypes(program, sub, out);
  }
}

async function writeJsonSchema(
  context: EmitContext<any>,
  types: Type[],
  outputDir: string,
  versionStr?: string,
): Promise<void> {
  const { program } = context;
  const emitContext = {
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
export async function emitJsonSchema(context: EmitContext<any>, outputDir: string): Promise<void> {
  const { program } = context;

  for (const unit of planEmission(program)) {
    const types: Type[] = [];
    if (unit.sharedNs) collectOwnTypes(program, unit.sharedNs, types);
    if (unit.ownOnly) {
      collectOwnTypes(program, unit.ns, types);
    } else {
      collectIdentifiedTypes(program, unit.ns, types);
    }

    await writeJsonSchema(context, types, unitOutputDir(outputDir, unit.path), unit.versionStr);
  }
}
