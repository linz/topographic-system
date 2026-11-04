import type { Namespace, Program } from '@typespec/compiler';
import { joinPaths } from '@typespec/compiler';
import { unsafe_mutateSubgraphWithNamespace } from '@typespec/compiler/experimental';
import { getVersioningMutators } from '@typespec/versioning';

import { getChildNamespaces, resolveNamespace } from './utils.ts';

/** Namespace products (e.g. `NzTopo50`, `NzTopo250`) live as direct children of this namespace, unless overridden via the `root-namespace` emitter option. */
export const DefaultRootNamespace = 'Linz.Topography';

/** Builds the `namespace={dottedPath}` directory segment — the one place this format is defined. */
function namespaceSegment(dottedPath: string): string {
  return `namespace=${dottedPath}`;
}

/** The version directory segment is just the raw version string (e.g. `v0.4`) — no extra prefix. */
function versionSegment(versionStr: string): string {
  return versionStr;
}

export interface EmissionUnit {
  /** Output path fragment relative to an emitter's output root, including any version segment. */
  path: string;
  /** `path` without the trailing version segment, e.g. `namespace=linz.topography.nztopo50`, or `''`. */
  namespaceDir: string;
  /** The version directory segment itself (e.g. `v0.4`), set only when versioned. */
  versionDir?: string;
  versionStr?: string;
  productName?: string;
  /** Dotted, lowercased namespace path (e.g. `linz.topography.nztopo50`), set only when `Linz.Topography` was resolved. */
  namespacePath?: string;
  /** This unit's own namespace: a product namespace, or the root namespace if there are no products. */
  ns: Namespace;
  /**
   * The shared root namespace for this version, set only when `ns` is a product namespace (its
   * own types, declared directly on it rather than inside any product, are shared by every
   * product at this version). Undefined when `ns` already IS the root (no product split).
   */
  sharedNs?: Namespace;
  /**
   * True for the standalone unit covering the root namespace's own (shared) types, emitted
   * alongside its product siblings rather than folded only into them.
   */
  ownOnly?: boolean;
}

function planNamespace(ns: Namespace, rootDottedPath: string | undefined): EmissionUnit[] {
  const products = getChildNamespaces(ns);

  // A `namespace={dotted path}/` directory only earns its keep when there's more than one
  // product namespace to disambiguate between; a single (the root's own) namespace stays flat.
  if (products.length === 0) {
    return [{ path: '', namespaceDir: '', ns, namespacePath: rootDottedPath }];
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
 * `rootNamespace` (default `Linz.Topography`, or the global namespace if that namespace isn't
 * found). Each product sub-namespace (e.g. `NzTopo50`, `NzTopo250`) gets its own
 * `namespace={dotted path}/` directory to disambiguate it from its siblings — a product's
 * directory combines its own types with any declared directly on the shared root. With no
 * product split, there's nothing to disambiguate, so the root's own types stay flat (no
 * `namespace=` segment) regardless of whether `rootNamespace` resolved. If the root is versioned
 * via `@typespec/versioning`, each namespace's directory additionally nests a `{version}/`
 * directory per release (e.g. `v0.4/`).
 */
export function planEmission(program: Program, rootNamespace: string = DefaultRootNamespace): EmissionUnit[] {
  const rootNs = resolveNamespace(program.getGlobalNamespaceType(), rootNamespace);
  const targetNs = rootNs ?? program.getGlobalNamespaceType();
  const rootDottedPath = rootNs ? rootNamespace.toLowerCase() : undefined;

  const mutators = getVersioningMutators(program, targetNs);

  if (mutators && mutators.kind === 'versioned') {
    const units: EmissionUnit[] = [];
    for (const snapshot of mutators.snapshots) {
      const versionStr = String(snapshot.version.value ?? snapshot.version.name);
      const { type: mutatedType } = unsafe_mutateSubgraphWithNamespace(program, [snapshot.mutator], targetNs);
      // `mutateSubgraphWithNamespace` always returns a `Namespace` when given one; the broader
      // `MutableTypeWithNamespace` return type only exists because the function also folds in the
      // (unrelated) mutation of non-namespace types elsewhere in the library.
      if (mutatedType.kind !== 'Namespace') continue;
      const versionDir = versionSegment(versionStr);
      for (const unit of planNamespace(mutatedType, rootDottedPath)) {
        units.push({ ...unit, versionStr, versionDir, path: joinPaths(unit.namespaceDir, versionDir) });
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
