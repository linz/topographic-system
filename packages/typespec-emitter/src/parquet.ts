import { emitFile, resolvePath } from '@typespec/compiler';
import type { Model, Program, Type } from '@typespec/compiler';
import { isJsonSchemaDeclaration } from '@typespec/json-schema';

import { toSnakeCase, unwrapNullableType } from './utils.ts';

interface ParquetField {
  name: string;
  repetition: 'REQUIRED' | 'OPTIONAL' | 'REPEATED';
  physical_type?: string;
  logical_type?: string;
  fields?: ParquetField[];
}

function mapTypeSpecToParquet(type: Type): { physical_type?: string; logical_type?: string } {
  switch (type.kind) {
    case 'Scalar':
      switch (type.name) {
        case 'boolean':
          return { physical_type: 'BOOLEAN' };
        case 'int32':
          return { physical_type: 'INT32' };
        case 'int64':
          return { physical_type: 'INT64' };
        case 'float32':
          return { physical_type: 'FLOAT' };
        case 'float64':
          return { physical_type: 'DOUBLE' };
        case 'string':
          return { physical_type: 'BYTE_ARRAY', logical_type: 'STRING' };
        case 'bytes':
          return { physical_type: 'BYTE_ARRAY' };
        case 'plainDate':
          return { physical_type: 'INT32', logical_type: 'DATE' };
        case 'utcDateTime':
        case 'offsetDateTime':
          return { physical_type: 'INT64', logical_type: 'TIMESTAMP' };
        case 'decimal':
          return { physical_type: 'BYTE_ARRAY', logical_type: 'DECIMAL' };
      }
      break;
    case 'Enum':
      return { physical_type: 'BYTE_ARRAY', logical_type: 'STRING' };
  }
  return { physical_type: 'BYTE_ARRAY', logical_type: 'STRING' }; // fallback
}

function getParquetFields(model: Model): ParquetField[] {
  const fields: ParquetField[] = [];
  for (const prop of model.properties.values()) {
    const fieldName = toSnakeCase(prop.name);

    // Check nullability/optionality
    const unwrapped = unwrapNullableType(prop.type);
    const isOptional = prop.optional || unwrapped.optional;
    const repetition = isOptional ? 'OPTIONAL' : 'REQUIRED';

    const targetType = unwrapped.type;

    if (targetType.kind === 'Model' && targetType.name === 'Array' && targetType.indexer) {
      const arrayItemUnwrapped = unwrapNullableType(targetType.indexer.value);
      const arrayItemType = arrayItemUnwrapped.type;

      const itemField: ParquetField = {
        name: 'element',
        repetition: arrayItemUnwrapped.optional ? 'OPTIONAL' : 'REQUIRED',
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
        if (mapped.physical_type) itemField.physical_type = mapped.physical_type;
        if (mapped.logical_type) itemField.logical_type = mapped.logical_type;
      }

      fields.push({
        name: fieldName,
        repetition,
        logical_type: 'LIST',
        fields: [
          {
            name: 'list',
            repetition: 'REPEATED',
            fields: [itemField],
          },
        ],
      });
    } else if (
      targetType.kind === 'Model' &&
      targetType.name !== 'Array' &&
      targetType.name !== 'Record' &&
      targetType.properties.size > 0
    ) {
      fields.push({
        name: fieldName,
        repetition,
        fields: getParquetFields(targetType),
      });
    } else {
      const mapped = mapTypeSpecToParquet(targetType);
      const field: ParquetField = {
        name: fieldName,
        repetition,
      };
      if (mapped.physical_type) field.physical_type = mapped.physical_type;
      if (mapped.logical_type) field.logical_type = mapped.logical_type;
      fields.push(field);
    }
  }
  return fields;
}

export async function emitParquetSchema(program: Program, outputDir: string, models: Map<string, Model>) {
  for (const [name, model] of models.entries()) {
    if (!isJsonSchemaDeclaration(program, model)) {
      continue;
    }
    const tableName = toSnakeCase(name);
    const schema = {
      type: 'message',
      name: tableName,
      fields: getParquetFields(model),
    };
    const schemaFile = resolvePath(outputDir, `${tableName}.json`);
    await emitFile(program, {
      path: schemaFile,
      content: JSON.stringify(schema, null, 2) + '\n',
    });
  }
}
