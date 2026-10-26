import type { Namespace, Model, Enum, Union, Type } from '@typespec/compiler';

export function toPascalCase(str: string): string {
  if (!str) return '';
  return str
    .split(/[_-]/)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join('');
}

export function toSnakeCase(str: string): string {
  if (!str) return '';
  return str
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .toLowerCase();
}

export function unwrapNullableType(type: Type): { type: Type; optional: boolean } {
  if (type.kind === 'Union') {
    const variants = Array.from(type.variants.values());
    const nonNullVariants = variants.filter((v) => v.type.kind !== 'Intrinsic' || v.type.name !== 'null');
    const hasNull = variants.length !== nonNullVariants.length;
    const first = nonNullVariants[0];
    if (first) {
      return { type: first.type, optional: hasNull };
    }
  }
  return { type, optional: false };
}

/** Resolves a dotted namespace path (e.g. `Linz.Topography`) from the global namespace. */
export function resolveNamespace(globalNs: Namespace, path: string): Namespace | undefined {
  let ns: Namespace | undefined = globalNs;
  for (const part of path.split('.')) {
    ns = ns?.namespaces.get(part);
    if (!ns) return undefined;
  }
  return ns;
}

/** Lists the immediate child namespaces of `ns`, excluding the built-in `TypeSpec` namespace. */
export function getChildNamespaces(ns: any): Array<{ name: string; ns: any }> {
  const out: Array<{ name: string; ns: any }> = [];
  if ('namespaces' in ns && ns.namespaces) {
    for (const sub of ns.namespaces.values()) {
      if (sub.name === 'TypeSpec') continue;
      out.push({ name: sub.name, ns: sub });
    }
  }
  return out;
}

/** Collects the models/enums/unions declared directly on `ns`, without recursing into children. */
export function collectOwnTypes(
  ns: any,
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
