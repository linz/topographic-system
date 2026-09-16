import { createAssetEmitter } from '@typespec/asset-emitter';
import type { EmitContext, Type } from '@typespec/compiler';
import { joinPaths } from '@typespec/compiler';
import { unsafe_mutateSubgraphWithNamespace } from '@typespec/compiler/experimental';
import { JsonSchemaEmitter, getId } from '@typespec/json-schema';
import { getVersioningMutators } from '@typespec/versioning';

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

export async function emitJsonSchema(context: EmitContext<any>, outputDir: string): Promise<void> {
  const { program } = context;
  const topoNs = program.getGlobalNamespaceType().namespaces.get('Topography');
  if (!topoNs) {
    throw new Error('Could not find Topography namespace in program.');
  }

  const mutators = getVersioningMutators(program, topoNs);
  if (!mutators || mutators.kind !== 'versioned') {
    throw new Error('Topography namespace is not versioned.');
  }

  for (const snapshot of mutators.snapshots) {
    const versionStr = String(snapshot.version.value ?? snapshot.version.name);
    const versionOutputDir = joinPaths(outputDir, `release=v${versionStr}`);

    const { type: mutatedNs } = unsafe_mutateSubgraphWithNamespace(program, [snapshot.mutator], topoNs);

    const emitContext = {
      ...context,
      emitterOutputDir: versionOutputDir,
      options: { ...context.options, 'file-type': 'json', versionStr },
    };

    const emitter = createAssetEmitter(program, VersionedJsonSchemaEmitter, emitContext);

    const typesToEmit: Type[] = [];
    if ('models' in mutatedNs && mutatedNs.models) {
      for (const m of mutatedNs.models.values()) {
        if (getId(program, m)) typesToEmit.push(m);
      }
    }
    if ('unions' in mutatedNs && mutatedNs.unions) {
      for (const u of mutatedNs.unions.values()) {
        if (getId(program, u)) typesToEmit.push(u);
      }
    }
    if ('enums' in mutatedNs && mutatedNs.enums) {
      for (const e of mutatedNs.enums.values()) {
        if (getId(program, e)) typesToEmit.push(e);
      }
    }
    if ('scalars' in mutatedNs && mutatedNs.scalars) {
      for (const s of mutatedNs.scalars.values()) {
        if (getId(program, s)) typesToEmit.push(s);
      }
    }

    for (const item of typesToEmit) {
      emitter.emitType(item);
    }

    await emitter.writeOutput();
  }
}
