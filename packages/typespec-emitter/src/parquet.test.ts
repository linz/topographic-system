import assert from 'node:assert';
import { describe, it } from 'node:test';

import type { SchemaElement } from 'hyparquet';

/**
 * Helper function to validate that a hyparquet schema tree matches our emitted Parquet JSON schema.
 */
export function validateHyparquetSchema(
  hyparquetTree: { element: SchemaElement; children: { element: SchemaElement }[] },
  expectedSchema: {
    name: string;
    fields: Array<{ name: string; repetition: string; physical_type?: string; logical_type?: string }>;
  },
): void {
  assert.strictEqual(
    hyparquetTree.element.name,
    expectedSchema.name,
    `Expected table name ${expectedSchema.name}, got ${hyparquetTree.element.name}`,
  );

  const hyparquetColumns = new Map(hyparquetTree.children.map((c) => [c.element.name, c.element]));

  for (const field of expectedSchema.fields) {
    const actual = hyparquetColumns.get(field.name);
    assert.ok(actual, `Column "${field.name}" missing in hyparquet schema`);

    assert.strictEqual(actual.repetition_type, field.repetition, `Repetition mismatch for column "${field.name}"`);

    if (field.physical_type) {
      assert.strictEqual(actual.type, field.physical_type, `Physical type mismatch for column "${field.name}"`);
    }

    if (field.logical_type) {
      assert.strictEqual(
        actual.logical_type?.type,
        field.logical_type,
        `Logical type mismatch for column "${field.name}"`,
      );
    }
  }
}

describe('Parquet Schema Emitter & Validator', () => {
  it('validates a hyparquet schema tree against an emitted Parquet JSON schema', () => {
    const mockHyparquetTree = {
      element: { name: 'railway_line', num_children: 2 },
      children: [
        {
          element: {
            name: 'id',
            type: 'BYTE_ARRAY',
            repetition_type: 'REQUIRED',
            logical_type: { type: 'STRING' },
          },
        },
        {
          element: {
            name: 'route',
            type: 'BYTE_ARRAY',
            repetition_type: 'OPTIONAL',
            logical_type: { type: 'STRING' },
          },
        },
      ],
    };

    const sampleEmittedSchema = {
      type: 'message',
      name: 'railway_line',
      fields: [
        {
          name: 'id',
          repetition: 'REQUIRED',
          physical_type: 'BYTE_ARRAY',
          logical_type: 'STRING',
        },
        {
          name: 'route',
          repetition: 'OPTIONAL',
          physical_type: 'BYTE_ARRAY',
          logical_type: 'STRING',
        },
      ],
    };

    validateHyparquetSchema(mockHyparquetTree as any, sampleEmittedSchema);
  });

  it('fails validation when a required column is missing', () => {
    const mockHyparquetTree = {
      element: { name: 'railway_line', num_children: 1 },
      children: [
        {
          element: {
            name: 'id',
            type: 'BYTE_ARRAY',
            repetition_type: 'REQUIRED',
            logical_type: { type: 'STRING' },
          },
        },
      ],
    };

    const sampleEmittedSchema = {
      type: 'message',
      name: 'railway_line',
      fields: [
        {
          name: 'id',
          repetition: 'REQUIRED',
          physical_type: 'BYTE_ARRAY',
          logical_type: 'STRING',
        },
        {
          name: 'route',
          repetition: 'OPTIONAL',
          physical_type: 'BYTE_ARRAY',
          logical_type: 'STRING',
        },
      ],
    };

    assert.throws(() => {
      validateHyparquetSchema(mockHyparquetTree as any, sampleEmittedSchema);
    }, /Column "route" missing/);
  });
});
