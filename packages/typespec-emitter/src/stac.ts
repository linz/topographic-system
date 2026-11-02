import type { EmitContext, Program } from '@typespec/compiler';
import { emitFile, joinPaths, resolvePath } from '@typespec/compiler';
import { getId } from '@typespec/json-schema';

import type { EmissionUnit } from './plan.ts';
import { LATEST_DIR, planEmission, unitOutputDir } from './plan.ts';
import { getChildNamespaces } from './utils.ts';

/** Collects `@jsonSchema` ids declared directly on `ns` (no recursion into sub-namespaces). */
function collectOwnAssetNames(program: Program, ns: any, out: string[]): void {
  if ('models' in ns && ns.models) {
    for (const m of ns.models.values()) {
      const id = getId(program, m);
      if (id) out.push(id);
    }
  }
  if ('unions' in ns && ns.unions) {
    for (const u of ns.unions.values()) {
      const id = getId(program, u);
      if (id) out.push(id);
    }
  }
}

/** Collects `@jsonSchema` ids reachable from `ns`, recursing into sub-namespaces. */
function collectAssetNames(program: Program, ns: any, out: string[]): void {
  collectOwnAssetNames(program, ns, out);
  for (const { ns: sub } of getChildNamespaces(ns)) {
    collectAssetNames(program, sub, out);
  }
}

function assetNamesForUnit(program: Program, unit: EmissionUnit): string[] {
  const assetNames: string[] = [];
  if (unit.sharedNs) collectOwnAssetNames(program, unit.sharedNs, assetNames);
  if (unit.ownOnly) collectOwnAssetNames(program, unit.ns, assetNames);
  else collectAssetNames(program, unit.ns, assetNames);
  return [...new Set(assetNames)].sort();
}

async function emitCollection(
  program: Program,
  assetNames: string[],
  outputDir: string,
  opts: {
    id: string;
    title: string;
    description: string;
    rootHref: string;
    parentHref: string;
    assetHrefPrefix?: string;
  },
): Promise<void> {
  const assetHrefPrefix = opts.assetHrefPrefix ?? './';
  const assets: Record<string, { href: string; title: string; type: string; roles: string[] }> = {};
  for (const name of assetNames) {
    assets[name] = {
      href: `${assetHrefPrefix}${name}.json`,
      title: `${name} JSON Schema`,
      type: 'application/schema+json',
      roles: ['schema'],
    };
    assets[`${name}_parquet`] = {
      href: `${assetHrefPrefix}${name}.parquet.json`,
      title: `${name} Parquet Schema`,
      type: 'application/json',
      roles: ['schema'],
    };
  }

  const collection = {
    type: 'Collection',
    stac_version: '1.0.0',
    id: opts.id,
    title: opts.title,
    description: opts.description,
    license: 'CC-BY-4.0',
    extent: {
      spatial: { bbox: [[166.0, -47.5, 179.0, -34.0]] },
      temporal: { interval: [['2026-01-01T00:00:00Z', null]] },
    },
    links: [
      { rel: 'root', href: opts.rootHref, type: 'application/json' },
      { rel: 'parent', href: opts.parentHref, type: 'application/json' },
      { rel: 'self', href: './collection.json', type: 'application/json' },
    ],
    assets,
  };

  await emitFile(program, {
    path: resolvePath(outputDir, 'collection.json'),
    content: JSON.stringify(collection, null, 2) + '\n',
  });
}

function describeNamespace(unit: EmissionUnit): { id: string; title: string; description: string } {
  if (unit.ownOnly) {
    return { id: 'schema', title: 'Topographic Schemas', description: 'LINZ Topographic schema definitions (shared)' };
  }
  if (unit.productName) {
    return {
      id: `schema_${unit.productName.toLowerCase()}`,
      title: `Topographic Schemas — ${unit.productName}`,
      description: `LINZ Topographic schema definitions (${unit.productName})`,
    };
  }
  return { id: 'schema', title: 'Topographic Schemas', description: 'LINZ Topographic schema definitions' };
}

/**
 * Emits STAC metadata from the same (namespace × version) units every other emitter writes into —
 * see `planEmission`. Each distinct namespace directory gets a `collection.json` per release (or a
 * single one directly, if unversioned) plus, if versioned, a `catalog.json` linking its releases.
 * If there's more than one namespace (the shared root plus its products), a root `catalog.json`
 * links each namespace's entry point; with only one namespace, that namespace's own
 * catalog/collection is written directly into `outputDir` with no extra wrapping catalog.
 */
export async function emitStacMetadata(context: EmitContext<any>, outputDir: string): Promise<void> {
  const { program } = context;
  const units = planEmission(program);

  const byNamespace = new Map<string, EmissionUnit[]>();
  for (const unit of units) {
    const list = byNamespace.get(unit.namespaceDir);
    if (list) list.push(unit);
    else byNamespace.set(unit.namespaceDir, [unit]);
  }

  const isFlat = byNamespace.size === 1;
  const topChildren: Array<{ rel: string; href: string; title: string; type: string }> = [];

  for (const [namespaceDir, nsUnits] of byNamespace) {
    const first = nsUnits[0];
    if (first == null) continue;
    const { id, title, description } = describeNamespace(first);
    const namespaceOutputDir = unitOutputDir(outputDir, namespaceDir);

    // An unversioned unit's collection lives directly in `namespaceOutputDir`: with only one
    // namespace there's no wrapping catalog above it (it IS the root); otherwise its parent and
    // root are both the single top-level catalog one directory up.
    const unversionedHref = isFlat ? './collection.json' : '../catalog.json';
    // A versioned unit's collection lives one directory deeper, inside `release=vX/` (or
    // `latest/`): its parent is always the namespace's own catalog one directory up, but its
    // root is that same catalog only when it's also the top (isFlat) — otherwise root is the
    // actual top-level catalog two directories up.
    const versionedParentHref = '../catalog.json';
    const versionedRootHref = isFlat ? '../catalog.json' : '../../catalog.json';

    const releaseChildren: Array<{ rel: string; href: string; title: string; type: string }> = [];
    let latestUnit: EmissionUnit | undefined;
    for (const unit of nsUnits) {
      const assetNames = assetNamesForUnit(program, unit);
      const isVersioned = unit.versionStr != null;

      await emitCollection(program, assetNames, unitOutputDir(outputDir, unit.path), {
        id: isVersioned ? `${id}_v${unit.versionStr}` : id,
        title: isVersioned ? `${title} — Release v${unit.versionStr}` : title,
        description,
        parentHref: isVersioned ? versionedParentHref : unversionedHref,
        rootHref: isVersioned ? versionedRootHref : unversionedHref,
      });

      if (isVersioned && unit.releaseDir) {
        releaseChildren.push({
          rel: 'child',
          href: `./${unit.releaseDir}/collection.json`,
          title: `Release v${unit.versionStr}`,
          type: 'application/json',
        });
        // `planEmission` walks versioning snapshots oldest-first, so the last unit is the latest release.
        latestUnit = unit;
      }
    }

    if (latestUnit && latestUnit.releaseDir) {
      const latestAssetNames = assetNamesForUnit(program, latestUnit);
      await emitCollection(program, latestAssetNames, joinPaths(namespaceOutputDir, LATEST_DIR), {
        id: `${id}_latest`,
        title: `${title} — Latest`,
        description,
        parentHref: versionedParentHref,
        rootHref: versionedRootHref,
        assetHrefPrefix: `../${latestUnit.releaseDir}/`,
      });
      releaseChildren.unshift({
        rel: 'child',
        href: `./${LATEST_DIR}/collection.json`,
        title: 'Latest',
        type: 'application/json',
      });
    }

    let entryFile = 'collection.json';
    if (releaseChildren.length > 0) {
      const catalog = {
        type: 'Catalog',
        stac_version: '1.0.0',
        id,
        title,
        description,
        links: [
          { rel: 'root', href: isFlat ? './catalog.json' : '../catalog.json', type: 'application/json' },
          { rel: 'self', href: './catalog.json', type: 'application/json' },
          ...releaseChildren,
        ],
      };
      await emitFile(program, {
        path: resolvePath(namespaceOutputDir, 'catalog.json'),
        content: JSON.stringify(catalog, null, 2) + '\n',
      });
      entryFile = 'catalog.json';
    }

    if (!isFlat) {
      topChildren.push({ rel: 'child', href: `./${namespaceDir}/${entryFile}`, title, type: 'application/json' });
    }
  }

  if (!isFlat) {
    const catalog = {
      type: 'Catalog',
      stac_version: '1.0.0',
      id: 'schema',
      title: 'Topographic Schemas',
      description: 'Catalog of LINZ Topographic schema namespaces',
      links: [
        { rel: 'root', href: './catalog.json', type: 'application/json' },
        { rel: 'self', href: './catalog.json', type: 'application/json' },
        ...topChildren,
      ],
    };
    await emitFile(program, {
      path: resolvePath(outputDir, 'catalog.json'),
      content: JSON.stringify(catalog, null, 2) + '\n',
    });
  }
}
