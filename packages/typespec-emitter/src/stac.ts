import { emitFile, joinPaths, resolvePath } from '@typespec/compiler';
import type { EmitContext } from '@typespec/compiler';
import { unsafe_mutateSubgraphWithNamespace } from '@typespec/compiler/experimental';
import { getId } from '@typespec/json-schema';
import { getVersioningMutators } from '@typespec/versioning';

export async function emitStacMetadata(context: EmitContext<any>, outputDir: string): Promise<void> {
  const { program } = context;
  const topoNs = program.getGlobalNamespaceType().namespaces.get('Topography');
  if (!topoNs) {
    return;
  }

  const mutators = getVersioningMutators(program, topoNs);
  if (!mutators || mutators.kind !== 'versioned') {
    return;
  }

  const catalogChildren: Array<{ rel: string; href: string; title: string; type: string }> = [];

  for (const snapshot of mutators.snapshots) {
    const versionStr = String(snapshot.version.value ?? snapshot.version.name);
    const versionOutputDir = joinPaths(outputDir, `release=v${versionStr}`);

    catalogChildren.push({
      rel: 'child',
      href: `./release=v${versionStr}/collection.json`,
      title: `Release v${versionStr}`,
      type: 'application/json',
    });

    const { type: mutatedNs } = unsafe_mutateSubgraphWithNamespace(program, [snapshot.mutator], topoNs);

    const assetNames: string[] = [];
    if ('models' in mutatedNs && mutatedNs.models) {
      for (const m of mutatedNs.models.values()) {
        const id = getId(program, m);
        if (id) assetNames.push(id);
      }
    }
    if ('unions' in mutatedNs && mutatedNs.unions) {
      for (const u of mutatedNs.unions.values()) {
        const id = getId(program, u);
        if (id) assetNames.push(id);
      }
    }

    assetNames.sort();

    const assets: Record<string, { href: string; title: string; type: string; roles: string[] }> = {};
    for (const name of assetNames) {
      assets[name] = {
        href: `./${name}.json`,
        title: `${name} JSON Schema`,
        type: 'application/schema+json',
        roles: ['schema'],
      };
      assets[`${name}_parquet`] = {
        href: `./${name}.parquet.json`,
        title: `${name} Parquet Schema`,
        type: 'application/json',
        roles: ['schema'],
      };
    }

    const collection = {
      type: 'Collection',
      stac_version: '1.0.0',
      id: `schema_v${versionStr}`,
      title: `Topographic Schema Release v${versionStr}`,
      description: `LINZ Topographic schema definitions for release v${versionStr}`,
      license: 'CC-BY-4.0',
      extent: {
        spatial: { bbox: [[166.0, -47.5, 179.0, -34.0]] },
        temporal: { interval: [['2026-01-01T00:00:00Z', null]] },
      },
      links: [
        { rel: 'root', href: '../catalog.json', type: 'application/json' },
        { rel: 'parent', href: '../catalog.json', type: 'application/json' },
        { rel: 'self', href: './collection.json', type: 'application/json' },
      ],
      assets,
    };

    const collectionFile = resolvePath(versionOutputDir, 'collection.json');
    await emitFile(program, { path: collectionFile, content: JSON.stringify(collection, null, 2) + '\n' });
  }

  const catalog = {
    type: 'Catalog',
    stac_version: '1.0.0',
    id: 'schema',
    title: 'Topographic Schemas',
    description: 'Catalog of LINZ Topographic Schema releases',
    links: [
      { rel: 'root', href: './catalog.json', type: 'application/json' },
      { rel: 'self', href: './catalog.json', type: 'application/json' },
      ...catalogChildren,
    ],
  };

  const catalogFile = resolvePath(outputDir, 'catalog.json');
  await emitFile(program, { path: catalogFile, content: JSON.stringify(catalog, null, 2) + '\n' });
}
