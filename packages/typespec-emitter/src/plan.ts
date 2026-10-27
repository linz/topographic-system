import type { Program } from '@typespec/compiler';
import { joinPaths } from '@typespec/compiler';
import { unsafe_mutateSubgraphWithNamespace } from '@typespec/compiler/experimental';
import { getVersioningMutators } from '@typespec/versioning';

import { getChildNamespaces, resolveNamespace } from './utils.ts';

/** Namespace products (e.g. `NzTopo50`, `NzTopo250`) live as direct children of this namespace. */
export const ROOT_NAMESPACE = 'Linz.Topography';

/** Name of the mutable "latest release" directory nested inside a versioned namespace directory. */
export const LATEST_DIR = 'latest';

/** Builds the `namespace={dottedPath}` directory segment — the one place this format is defined. */
function namespaceSegment(dottedPath: string): string {
  return `namespace=${dottedPath}`;
}

/** Builds the `release=v{version}` directory segment — the one place this format is defined. */
function releaseSegment(versionStr: string): string {
  return `release=v${versionStr}`;
}

export interface EmissionUnit {
  /** Output path fragment relative to an emitter's output root, including any release segment. */
  path: string;
  /** `path` without the trailing release segment, e.g. `namespace=linz.topography.nztopo50`, or `''`. */
  namespaceDir: string;
  /** The release directory segment itself (e.g. `release=v1`), set only when versioned. */
  releaseDir?: string;
  versionStr?: string;
  productName?: string;
  /** Dotted, lowercased namespace path (e.g. `linz.topography.nztopo50`), set only when `Linz.Topography` was resolved. */
  namespacePath?: string;
  /** This unit's own namespace: a product namespace, or the root namespace if there are no products. */
  ns: any;
  /**
   * The shared root namespace for this version, set only when `ns` is a product namespace (its
   * own types, declared directly on it rather than inside any product, are shared by every
   * product at this version). Undefined when `ns` already IS the root (no product split).
   */
  sharedNs?: any;
  /**
   * True for the standalone unit covering the root namespace's own (shared) types, emitted
   * alongside its product siblings rather than folded only into them.
   */
  ownOnly?: boolean;
}

function planNamespace(ns: any, rootDottedPath: string | undefined): EmissionUnit[] {
  const products = getChildNamespaces(ns);

  if (products.length === 0) {
    const namespaceDir = rootDottedPath ? namespaceSegment(rootDottedPath) : '';
    return [{ path: namespaceDir, namespaceDir, ns, namespacePath: rootDottedPath }];
  }

  const units: EmissionUnit[] = [];

  // The root's own (shared) types get a standalone unit too, alongside each product.
  if (rootDottedPath) {
    const namespaceDir = namespaceSegment(rootDottedPath);
    units.push({ path: namespaceDir, namespaceDir, ns, namespacePath: rootDottedPath, ownOnly: true });
  }

  for (const product of products) {
    const namespacePath = rootDottedPath ? `${rootDottedPath}.${product.name.toLowerCase()}` : undefined;
    const namespaceDir = namespaceSegment(namespacePath ?? product.name.toLowerCase());
    units.push({
      path: namespaceDir,
      namespaceDir,
      productName: product.name,
      ns: product.ns,
      sharedNs: ns,
      namespacePath,
    });
  }

  return units;
}

/**
 * Computes the (namespace × version) output units every emitter should write into, resolving
 * `Linz.Topography` (or the global namespace if there is no `Linz.Topography` namespace). Each
 * namespace reachable this way — the root itself (if it has product children) plus each product
 * sub-namespace — gets its own `namespace={dotted path}/` directory; a product's directory
 * combines its own types with any declared directly on the shared root. If the root is versioned
 * via `@typespec/versioning`, each namespace's directory additionally nests a
 * `release=v{version}/` per release. With no versioning and no products, a single unit covers
 * everything, at `namespace={dotted path}/` (or `''` if `Linz.Topography` wasn't found at all).
 */
export function planEmission(program: Program): EmissionUnit[] {
  const rootNs = resolveNamespace(program.getGlobalNamespaceType(), ROOT_NAMESPACE);
  const targetNs = rootNs ?? program.getGlobalNamespaceType();
  const rootDottedPath = rootNs ? ROOT_NAMESPACE.toLowerCase() : undefined;

  const mutators = getVersioningMutators(program, targetNs);

  if (mutators && mutators.kind === 'versioned') {
    const units: EmissionUnit[] = [];
    for (const snapshot of mutators.snapshots) {
      const versionStr = String(snapshot.version.value ?? snapshot.version.name);
      const { type: mutatedNs } = unsafe_mutateSubgraphWithNamespace(program, [snapshot.mutator], targetNs);
      const releaseDir = releaseSegment(versionStr);
      for (const unit of planNamespace(mutatedNs, rootDottedPath)) {
        units.push({ ...unit, versionStr, releaseDir, path: joinPaths(unit.namespaceDir, releaseDir) });
      }
    }
    return units;
  }

  return planNamespace(targetNs, rootDottedPath);
}

/** Joins `path` onto `base`, leaving `base` untouched when `path` is empty (the flat/top-level case). */
export function unitOutputDir(base: string, path: string): string {
  return path ? joinPaths(base, path) : base;
}
