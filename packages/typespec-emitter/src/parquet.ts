import { emitFile, resolvePath } from '@typespec/compiler';
import type { EmitContext, Model, Type, Union } from '@typespec/compiler';
import { getId } from '@typespec/json-schema';

import { planEmission, unitOutputDir } from './plan.ts';
import { getChildNamespaces, toSnakeCase, unwrapNullableType } from './utils.ts';

export interface ParquetField {
  name: string;
  repetition_type: 'REQUIRED' | 'OPTIONAL' | 'REPEATED';
  type?: string;
  logical_type?: string;
  fields?: ParquetField[];
}

function mapTypeSpecToParquet(type: Type): { type?: string; logical_type?: string } {
  switch (type.kind) {
    case 'Scalar':
      switch (type.name) {
        case 'boolean':
          return { type: 'BOOLEAN' };
        case 'int8':
        case 'uint8':
        case 'int16':
        case 'uint16':
        case 'int32':
        case 'uint32':
        case 'integer':
          return { type: 'INT32' };
        case 'int64':
        case 'uint64':
          return { type: 'INT64' };
        case 'float32':
          return { type: 'FLOAT' };
        case 'float64':
        case 'numeric':
          return { type: 'DOUBLE' };
        case 'string':
        case 'url':
          return { type: 'BYTE_ARRAY', logical_type: 'STRING' };
        case 'bytes':
          return { type: 'BYTE_ARRAY' };
        case 'plainDate':
          return { type: 'INT32', logical_type: 'DATE' };
        case 'utcDateTime':
        case 'offsetDateTime':
          return { type: 'INT64', logical_type: 'TIMESTAMP' };
        case 'decimal':
          return { type: 'BYTE_ARRAY', logical_type: 'DECIMAL' };
      }
      break;
    case 'Enum':
    case 'String':
      return { type: 'BYTE_ARRAY', logical_type: 'STRING' };
    case 'Intrinsic':
      if (type.name === 'null') {
        return {};
      }
      return { type: 'BYTE_ARRAY' };
  }
  return { type: 'BYTE_ARRAY', logical_type: 'STRING' }; // fallback
}

export function getParquetFields(model: Model): ParquetField[] {
  const fields: ParquetField[] = [];
  for (const prop of model.properties.values()) {
    const fieldName = toSnakeCase(prop.name);

    // Check nullability/optionality
    const unwrapped = unwrapNullableType(prop.type);
    const isExplicitNull = unwrapped.type.kind === 'Intrinsic' && unwrapped.type.name === 'null';
    const isOptional = prop.optional || unwrapped.optional || isExplicitNull;
    const repetition = isOptional ? 'OPTIONAL' : 'REQUIRED';

    const targetType = unwrapped.type;

    if (targetType.kind === 'Model' && targetType.name === 'Array' && targetType.indexer) {
      const arrayItemUnwrapped = unwrapNullableType(targetType.indexer.value);
      const arrayItemType = arrayItemUnwrapped.type;

      const itemField: ParquetField = {
        name: 'element',
        repetition_type: arrayItemUnwrapped.optional ? 'OPTIONAL' : 'REQUIRED',
      };

      if (
        arrayItemType.kind === 'Model' &&
        arrayItemType.name !== 'Array' &&
        arrayItemType.name !== 'Record' &&
        arrayItemType.properties.size > 0
      ) {
        itemField.fields = getParquetFields(arrayItemType);
      } else {
        const mapped = mapTypeSpecToParquet(arrayItemType);
        if (mapped.type) itemField.type = mapped.type;
        if (mapped.logical_type) itemField.logical_type = mapped.logical_type;
      }

      fields.push({
        name: fieldName,
        repetition_type: repetition,
        logical_type: 'LIST',
        fields: [{ name: 'list', repetition_type: 'REPEATED', fields: [itemField] }],
      });
    } else if (
      targetType.kind === 'Model' &&
      targetType.name !== 'Array' &&
      targetType.name !== 'Record' &&
      targetType.properties.size > 0
    ) {
      fields.push({ name: fieldName, repetition_type: repetition, fields: getParquetFields(targetType) });
    } else {
      const mapped = mapTypeSpecToParquet(targetType);
      const field: ParquetField = { name: fieldName, repetition_type: repetition };
      if (mapped.type) field.type = mapped.type;
      if (mapped.logical_type) field.logical_type = mapped.logical_type;
      fields.push(field);
    }
  }
  return fields;
}

export function getParquetFieldsForUnion(union: Union): ParquetField[] {
  const fieldsMap = new Map<string, ParquetField>();
  const variantModels: Model[] = [];

  for (const variant of union.variants.values()) {
    const unwrapped = unwrapNullableType(variant.type);
    if (unwrapped.type.kind === 'Model') {
      variantModels.push(unwrapped.type);
    }
  }

  if (variantModels.length === 0) {
    return [];
  }

  const propVariantCount = new Map<string, number>();

  for (const model of variantModels) {
    const fields = getParquetFields(model);
    for (const f of fields) {
      propVariantCount.set(f.name, (propVariantCount.get(f.name) ?? 0) + 1);
      const existing = fieldsMap.get(f.name);
      if (existing == null) {
        fieldsMap.set(f.name, { ...f });
      } else {
        if (existing.type == null && f.type != null) {
          existing.type = f.type;
          existing.logical_type = f.logical_type;
        }
        if (f.repetition_type === 'OPTIONAL' || existing.repetition_type === 'OPTIONAL') {
          existing.repetition_type = 'OPTIONAL';
        }
      }
    }
  }

  for (const [name, count] of propVariantCount.entries()) {
    if (count < variantModels.length) {
      const field = fieldsMap.get(name);
      if (field != null) {
        field.repetition_type = 'OPTIONAL';
      }
    }
  }

  return Array.from(fieldsMap.values()).filter((f) => Boolean(f.type || f.fields));
}

interface NamedTypeToEmit {
  name: string;
  type: Model | Union;
}

/** Collects `@jsonSchema`-tagged models/unions declared directly on `ns`. */
function collectOwnTypes(program: any, ns: any, out: NamedTypeToEmit[]): void {
  if ('models' in ns && ns.models) {
    for (const m of ns.models.values()) {
      const id = getId(program, m);
      if (id) out.push({ name: id, type: m });
    }
  }
  if ('unions' in ns && ns.unions) {
    for (const u of ns.unions.values()) {
      const id = getId(program, u);
      if (id) out.push({ name: id, type: u });
    }
  }
}

/** Collects `@jsonSchema`-tagged models/unions reachable from `ns`, recursing into sub-namespaces. */
function collectIdentifiedTypes(program: any, ns: any, out: NamedTypeToEmit[]): void {
  collectOwnTypes(program, ns, out);
  for (const { ns: sub } of getChildNamespaces(ns)) {
    collectIdentifiedTypes(program, sub, out);
  }
}

/**
 * Emits a Parquet schema for every `@jsonSchema`-tagged model/union, one output per
 * (version × product) unit — see `planEmission`. Each unit combines its own namespace's types
 * with any declared directly on the shared root at that version.
 */
export async function emitParquetSchema(context: EmitContext<any>, outputDir: string): Promise<void> {
  const { program } = context;

  for (const unit of planEmission(program)) {
    const typesToEmit: NamedTypeToEmit[] = [];
    if (unit.sharedNs) collectOwnTypes(program, unit.sharedNs, typesToEmit);
    if (unit.ownOnly) {
      collectOwnTypes(program, unit.ns, typesToEmit);
    } else {
      collectIdentifiedTypes(program, unit.ns, typesToEmit);
    }

    const unitDir = unitOutputDir(outputDir, unit.path);
    for (const item of typesToEmit) {
      const fields = item.type.kind === 'Union' ? getParquetFieldsForUnion(item.type) : getParquetFields(item.type);
      const schema = { type: 'message', name: item.name, fields };
      const schemaFile = resolvePath(unitDir, `${item.name}.parquet.json`);
      await emitFile(program, { path: schemaFile, content: JSON.stringify(schema, null, 2) + '\n' });
    }
  }
}
