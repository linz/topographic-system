import { emitFile, joinPaths, resolvePath } from '@typespec/compiler';
import type { EmitContext, Model, Type, Union } from '@typespec/compiler';
import { unsafe_mutateSubgraphWithNamespace } from '@typespec/compiler/experimental';
import { getId, isJsonSchemaDeclaration } from '@typespec/json-schema';
import { getVersioningMutators } from '@typespec/versioning';

import { toSnakeCase, unwrapNullableType } from './utils.ts';

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
      if (!existing) {
        fieldsMap.set(f.name, { ...f });
      } else {
        if (!existing.type && f.type) {
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
      if (field) {
        field.repetition_type = 'OPTIONAL';
      }
    }
  }

  return Array.from(fieldsMap.values()).filter((f) => Boolean(f.type || f.fields));
}

export async function emitParquetSchema(
  context: EmitContext<any>,
  outputDir: string,
  unversionedModels?: Map<string, Model>,
): Promise<void> {
  const { program } = context;
  const topoNs = program.getGlobalNamespaceType().namespaces.get('Topography');

  if (topoNs) {
    const mutators = getVersioningMutators(program, topoNs);
    if (mutators && mutators.kind === 'versioned') {
      for (const snapshot of mutators.snapshots) {
        const versionStr = String(snapshot.version.value ?? snapshot.version.name);
        const versionOutputDir = joinPaths(outputDir, `release=v${versionStr}`);

        const { type: mutatedNs } = unsafe_mutateSubgraphWithNamespace(program, [snapshot.mutator], topoNs);

        const typesToEmit: Array<{ name: string; type: Model | Union }> = [];
        if ('models' in mutatedNs && mutatedNs.models) {
          for (const m of mutatedNs.models.values()) {
            const id = getId(program, m);
            if (id) typesToEmit.push({ name: id, type: m });
          }
        }
        if ('unions' in mutatedNs && mutatedNs.unions) {
          for (const u of mutatedNs.unions.values()) {
            const id = getId(program, u);
            if (id) typesToEmit.push({ name: id, type: u });
          }
        }

        for (const item of typesToEmit) {
          const fields = item.type.kind === 'Union' ? getParquetFieldsForUnion(item.type) : getParquetFields(item.type);
          const schema = { type: 'message', name: item.name, fields };
          const schemaFile = resolvePath(versionOutputDir, `${item.name}.parquet.json`);
          await emitFile(program, { path: schemaFile, content: JSON.stringify(schema, null, 2) + '\n' });
        }
      }
      return;
    }
  }

  // Unversioned fallback
  const models = unversionedModels ?? new Map<string, Model>();
  for (const [name, model] of models.entries()) {
    if (!isJsonSchemaDeclaration(program, model)) {
      continue;
    }
    const tableName = toSnakeCase(name);
    const schema = { type: 'message', name: tableName, fields: getParquetFields(model) };
    const schemaFile = resolvePath(outputDir, `${tableName}.parquet.json`);
    await emitFile(program, { path: schemaFile, content: JSON.stringify(schema, null, 2) + '\n' });
  }
}
