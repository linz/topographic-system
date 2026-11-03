import type { Model, Namespace, Enum, Program, Scalar, Type, Union } from '@typespec/compiler';
import { getId } from '@typespec/json-schema';

export function toPascalCase(str: string): string {
  if (str == null) return '';
  return str
    .split(/[_-]/)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join('');
}

export function toSnakeCase(str: string): string {
  if (str == null) return '';
  return str
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .toLowerCase();
}

/**
 * Strips a `null` variant from a union, e.g. `T | null` unwraps to `T`. A union with more than
 * one non-null variant (e.g. `string | int32`) isn't a simple nullable wrapper, so it's returned
 * as-is (still a `Union`) rather than silently collapsed to just its first variant.
 */
export function unwrapNullableType(type: Type): { type: Type; optional: boolean } {
  if (type.kind === 'Union') {
    const variants = Array.from(type.variants.values());
    const nonNullVariants = variants.filter((v) => v.type.kind !== 'Intrinsic' || v.type.name !== 'null');
    const hasNull = variants.length !== nonNullVariants.length;
    if (nonNullVariants.length === 1) {
      return { type: nonNullVariants[0]?.type as Type, optional: hasNull };
    }
    if (hasNull) {
      return { type, optional: true };
    }
  }
  return { type, optional: false };
}

/** Resolves a dotted namespace path (e.g. `Linz.Topography`) from the global namespace. */
export function resolveNamespace(globalNs: Namespace, path: string): Namespace | undefined {
  let ns: Namespace | undefined = globalNs;
  for (const part of path.split('.')) {
    ns = ns?.namespaces.get(part);
    if (ns == null) return undefined;
  }
  return ns;
}

/** Lists the immediate child namespaces of `ns`, excluding the built-in `TypeSpec` namespace. */
export function getChildNamespaces(ns: Namespace): Array<{ name: string; ns: Namespace }> {
  const out: Array<{ name: string; ns: Namespace }> = [];
  for (const sub of ns.namespaces.values()) {
    if (sub.name === 'TypeSpec') continue;
    out.push({ name: sub.name, ns: sub });
  }
  return out;
}

/** Collects the models/enums/unions declared directly on `ns`, without recursing into children. */
export function collectOwnTypes(
  ns: Namespace,
  models: Map<string, Model>,
  enums: Map<string, Enum>,
  unions: Map<string, Union>,
): void {
  for (const model of ns.models.values()) {
    if (model.name && model.name !== 'Array' && model.name !== 'Record') {
      models.set(model.name, model);
    }
  }
  for (const e of ns.enums.values()) {
    if (e.name) {
      enums.set(e.name, e);
    }
  }
  for (const u of ns.unions.values()) {
    if (u.name) {
      unions.set(u.name, u);
    }
  }
}

/** Collects every model/enum/union reachable from `ns`, recursing into sub-namespaces. */
export function collectTypes(
  ns: Namespace,
  models: Map<string, Model>,
  enums: Map<string, Enum>,
  unions: Map<string, Union>,
): void {
  collectOwnTypes(ns, models, enums, unions);
  for (const { ns: sub } of getChildNamespaces(ns)) {
    collectTypes(sub, models, enums, unions);
  }
}

/**
 * Visits every model/union/enum/scalar with a `@jsonSchema` id declared directly on `ns` — the
 * single place this set of kinds is enumerated, so JSON Schema/Parquet emission can't drift out of
 * sync on which kinds count as "identified" types.
 */
export function visitOwnIdentifiedTypes(
  program: Program,
  ns: Namespace,
  visit: (id: string, type: Model | Union | Enum | Scalar) => void,
): void {
  for (const m of ns.models.values()) {
    const id = getId(program, m);
    if (id) visit(id, m);
  }
  for (const u of ns.unions.values()) {
    const id = getId(program, u);
    if (id) visit(id, u);
  }
  for (const e of ns.enums.values()) {
    const id = getId(program, e);
    if (id) visit(id, e);
  }
  for (const s of ns.scalars.values()) {
    const id = getId(program, s);
    if (id) visit(id, s);
  }
}

/** Visits every model/union/enum/scalar with a `@jsonSchema` id reachable from `ns`, recursing into sub-namespaces. */
export function visitIdentifiedTypes(
  program: Program,
  ns: Namespace,
  visit: (id: string, type: Model | Union | Enum | Scalar) => void,
): void {
  visitOwnIdentifiedTypes(program, ns, visit);
  for (const { ns: sub } of getChildNamespaces(ns)) {
    visitIdentifiedTypes(program, sub, visit);
  }
}
