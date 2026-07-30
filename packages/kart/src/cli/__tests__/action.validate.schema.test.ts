import assert from 'node:assert';
import { describe, it } from 'node:test';

import type { ErrorObject } from 'ajv/dist/2020.js';

import type { ErrorAggregate } from '../action.validate.schema.ts';
import { collapseErrors, errorSample, MaxErrorSamples, summariseErrors } from '../action.validate.schema.ts';

function error(partial: Partial<ErrorObject>): ErrorObject {
  return {
    instancePath: '',
    schemaPath: '#',
    keyword: 'type',
    params: {},
    ...partial,
  } as ErrorObject;
}

// The three errors AJV emits for one bad value under `anyOf: [enum, null]`.
function nullableEnumErrors(instancePath: string, data: unknown): ErrorObject[] {
  return [
    error({ instancePath, keyword: 'enum', message: 'must be equal to one of the allowed values', data }),
    error({ instancePath, keyword: 'type', params: { type: 'null' }, message: 'must be null', data }),
    error({ instancePath, keyword: 'anyOf', message: 'must match a schema in anyOf', data }),
  ];
}

describe('action.validate.schema', () => {
  describe('collapseErrors', () => {
    it('collapses a nullable enum failure to the enum error', () => {
      const collapsed = collapseErrors(nullableEnumErrors('/font', 'Comic Sans'));
      assert.strictEqual(collapsed.length, 1);
      assert.strictEqual(collapsed[0]?.keyword, 'enum');
    });

    it('keeps errors for distinct properties separate', () => {
      const collapsed = collapseErrors([
        ...nullableEnumErrors('/font', 'Comic Sans'),
        ...nullableEnumErrors('/style', 'wibble'),
      ]);
      assert.deepStrictEqual(
        collapsed.map((e) => e.instancePath),
        ['/font', '/style'],
      );
    });

    it('drops standalone wrapper errors', () => {
      const collapsed = collapseErrors([error({ keyword: 'anyOf', message: 'must match a schema in anyOf' })]);
      assert.strictEqual(collapsed.length, 0);
    });

    it('preserves a lone null-type error when it is the only one for a property', () => {
      const collapsed = collapseErrors([
        error({ instancePath: '/colour', keyword: 'type', params: { type: 'null' }, message: 'must be null' }),
      ]);
      assert.strictEqual(collapsed.length, 1);
      assert.strictEqual(collapsed[0]?.message, 'must be null');
    });

    it('leaves unrelated single errors untouched', () => {
      const errors = [
        error({
          instancePath: '',
          keyword: 'required',
          params: { missingProperty: 'topo_id' },
          message: "must have required property 'topo_id'",
        }),
      ];
      assert.deepStrictEqual(collapseErrors(errors), errors);
    });
  });

  describe('errorSample', () => {
    it('returns the offending value for value-level errors', () => {
      assert.strictEqual(errorSample(error({ keyword: 'enum', data: 'Comic Sans' })), 'Comic Sans');
    });

    it('reports the extra column name for additionalProperties', () => {
      const er = error({
        keyword: 'additionalProperties',
        params: { additionalProperty: 'rogue_col' },
        data: { rogue_col: 1 },
      });
      assert.strictEqual(errorSample(er), 'rogue_col');
    });

    it('omits a sample when the message already names the property', () => {
      const er = error({ keyword: 'required', params: { missingProperty: 'topo_id' } });
      assert.strictEqual(errorSample(er), undefined);
    });

    it('omits a sample for object-valued data with no useful detail', () => {
      assert.strictEqual(errorSample(error({ keyword: 'type', data: { a: 1 } })), undefined);
    });

    it('truncates long values', () => {
      const long = 'x'.repeat(200);
      const sample = errorSample(error({ keyword: 'pattern', data: long }));
      assert.ok(sample && sample.length < long.length);
      assert.ok(sample?.endsWith('…'));
    });

    it('stringifies null and numeric values', () => {
      assert.strictEqual(errorSample(error({ keyword: 'type', data: null })), 'null');
      assert.strictEqual(errorSample(error({ keyword: 'type', data: 42 })), '42');
    });
  });

  describe('summariseErrors', () => {
    it('aggregates counts per distinct error across records', () => {
      const summary = new Map<string, ErrorAggregate>();
      summariseErrors(nullableEnumErrors('/font', 'Comic Sans'), summary);
      summariseErrors(nullableEnumErrors('/font', 'Wingdings'), summary);

      assert.strictEqual(summary.size, 1);
      const agg = summary.get('/font:must be equal to one of the allowed values');
      assert.strictEqual(agg?.count, 2);
      assert.deepStrictEqual(agg?.samples, ['Comic Sans', 'Wingdings']);
    });

    it('keeps only distinct samples', () => {
      const summary = new Map<string, ErrorAggregate>();
      summariseErrors(nullableEnumErrors('/font', 'Comic Sans'), summary);
      summariseErrors(nullableEnumErrors('/font', 'Comic Sans'), summary);

      assert.deepStrictEqual(summary.get('/font:must be equal to one of the allowed values')?.samples, ['Comic Sans']);
    });

    it('caps the number of samples but keeps counting', () => {
      const summary = new Map<string, ErrorAggregate>();
      for (let i = 0; i < MaxErrorSamples + 3; i++) {
        summariseErrors(nullableEnumErrors('/font', `font-${i}`), summary);
      }
      const agg = summary.get('/font:must be equal to one of the allowed values');
      assert.strictEqual(agg?.count, MaxErrorSamples + 3);
      assert.strictEqual(agg?.samples.length, MaxErrorSamples);
    });
  });
});
