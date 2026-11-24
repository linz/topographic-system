import { createHash } from 'node:crypto';

import { emitFile, resolvePath } from '@typespec/compiler';
import type { EmitContext, Enum, Model, Scalar, Type, Union } from '@typespec/compiler';

import type { EmitterOptions } from './index.ts';
import type { EmissionUnit } from './plan.ts';
import { unitOutputDir } from './plan.ts';
import { toSnakeCase, unwrapNullableType, visitIdentifiedTypes, visitOwnIdentifiedTypes } from './utils.ts';

/**
 * Matches `_NAMESPACE` in `packages/kart-import/src/kart_import/git/schema_ids.py` so the
 * TypeSpec-emitted Kart schema carries the exact same deterministic column IDs as `kart-import`.
 */
export const KART_COLUMN_ID_NAMESPACE = '57730ec2-ae17-4db6-9221-4b891d34ce05';

export type KartDataType = 'text' | 'integer' | 'float' | 'boolean' | 'geometry' | 'blob';

export interface KartColumn {
  id: string;
  name: string;
  dataType: KartDataType;
  size?: number;
  primaryKeyIndex?: number;
}

/** RFC 4122 UUIDv5 (SHA-1) matching Python's `uuid.uuid5(UUID(KART_COLUMN_ID_NAMESPACE), f"{theme}:{column}")`. */
export function stableColumnId(themeName: string, columnName: string): string {
  const nsBytes = Buffer.from(KART_COLUMN_ID_NAMESPACE.replace(/-/g, ''), 'hex');
  const nameBytes = Buffer.from(`${themeName}:${columnName}`, 'utf8');
  const hash = createHash('sha1').update(nsBytes).update(nameBytes).digest();
  hash[6] = (hash[6]! & 0x0f) | 0x50; // version 5
  hash[8] = (hash[8]! & 0x3f) | 0x80; // RFC 4122 variant
  const hex = hash.subarray(0, 16).toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

function mapTypeSpecToKart(type: Type): Pick<KartColumn, 'dataType' | 'size'> | null {
  switch (type.kind) {
    case 'Scalar':
      switch (type.name) {
        case 'boolean':
          return { dataType: 'boolean' };
        case 'int8':
        case 'uint8':
        case 'int16':
        case 'uint16':
        case 'int32':
        case 'uint32':
        case 'integer':
          return { dataType: 'integer', size: 32 };
        case 'int64':
        case 'uint64':
          return { dataType: 'integer', size: 64 };
        case 'float32':
          return { dataType: 'float', size: 32 };
        case 'float64':
        case 'numeric':
          return { dataType: 'float', size: 64 };
        case 'bytes':
          return { dataType: 'blob' };
        case 'string':
        case 'url':
        case 'plainDate':
        case 'utcDateTime':
        case 'offsetDateTime':
          return { dataType: 'text' };
      }
      break;
    case 'Enum':
    case 'EnumMember':
    case 'String':
      return { dataType: 'text' };
    case 'Intrinsic':
      if (type.name === 'null') return null;
      return { dataType: 'text' };
  }
  return { dataType: 'text' };
}

interface PendingColumn {
  name: string;
  spec: Pick<KartColumn, 'dataType' | 'size'> | null;
  primaryKeyIndex?: number;
}

function getPendingColumnsForModel(model: Model): PendingColumn[] {
  const cols: PendingColumn[] = [];
  for (const prop of model.properties.values()) {
    const colName = toSnakeCase(prop.name);
    // `bbox` is a GeoParquet 1.1 covering bbox added on export, never a column in Kart.
    if (colName === 'bbox') continue;

    if (colName === 'geometry') {
      cols.push({ name: 'geometry', spec: { dataType: 'geometry' } });
      continue;
    }

    const unwrapped = unwrapNullableType(prop.type);
    const spec = mapTypeSpecToKart(unwrapped.type);
    const col: PendingColumn = { name: colName, spec };
    if (colName === 'id') col.primaryKeyIndex = 0;
    cols.push(col);
  }
  return cols;
}

function finalizeColumns(themeName: string, pending: PendingColumn[]): KartColumn[] {
  return pending.map((col) => {
    const resolved = col.spec ?? { dataType: 'text' as const };
    const out: KartColumn = {
      id: stableColumnId(themeName, col.name),
      name: col.name,
      dataType: resolved.dataType,
    };
    if (resolved.size !== undefined) out.size = resolved.size;
    if (col.primaryKeyIndex !== undefined) out.primaryKeyIndex = col.primaryKeyIndex;
    return out;
  });
}

export function getKartColumns(themeName: string, model: Model): KartColumn[] {
  return finalizeColumns(themeName, getPendingColumnsForModel(model));
}

export function getKartColumnsForUnion(themeName: string, union: Union): KartColumn[] {
  const colsMap = new Map<string, PendingColumn>();
  for (const variant of union.variants.values()) {
    const unwrapped = unwrapNullableType(variant.type);
    if (unwrapped.type.kind !== 'Model') continue;
    for (const col of getPendingColumnsForModel(unwrapped.type)) {
      const existing = colsMap.get(col.name);
      if (existing == null) {
        colsMap.set(col.name, { ...col });
      } else if (existing.spec == null && col.spec != null) {
        existing.spec = col.spec;
      }
    }
  }
  return finalizeColumns(themeName, Array.from(colsMap.values()));
}

interface NamedTypeToEmit {
  name: string;
  type: Model | Union;
}

/**
 * Emits a Kart `schema.json` (`<theme>.kart.json`) for every `@jsonSchema`-tagged model/union,
 * one output per (version × product) unit — see `planEmission`.
 */
export async function emitKartSchema(
  context: EmitContext<EmitterOptions>,
  units: EmissionUnit[],
  outputDir: string,
): Promise<void> {
  const { program } = context;

  for (const unit of units) {
    const typesToEmit: NamedTypeToEmit[] = [];
    const visit = (id: string, type: Model | Union | Enum | Scalar): void => {
      if (type.kind === 'Model' || type.kind === 'Union') typesToEmit.push({ name: id, type });
    };
    if (unit.sharedNs) visitOwnIdentifiedTypes(program, unit.sharedNs, visit);
    if (unit.ownOnly) {
      visitOwnIdentifiedTypes(program, unit.ns, visit);
    } else {
      visitIdentifiedTypes(program, unit.ns, visit);
    }

    const unitDir = unitOutputDir(outputDir, unit.path);
    for (const item of typesToEmit) {
      const columns =
        item.type.kind === 'Union'
          ? getKartColumnsForUnion(item.name, item.type)
          : getKartColumns(item.name, item.type);
      const schemaFile = resolvePath(unitDir, `${item.name}.kart.json`);
      await emitFile(program, { path: schemaFile, content: JSON.stringify(columns, null, 2) + '\n' });
    }
  }
}

