import assert from 'node:assert';
import { describe, it } from 'node:test';

import { getKartColumns, getKartColumnsForUnion, stableColumnId } from '../kart.ts';

function mockProp(name: string, type: unknown) {
  return [name, { name, type }] as const;
}

describe('Kart Schema Emitter', () => {
  it('generates deterministic UUIDv5 matching Python schema_ids._stable_column_id', () => {
    // Verified against: uuid.uuid5(UUID("57730ec2-ae17-4db6-9221-4b891d34ce05"), "airport:id")
    assert.strictEqual(stableColumnId('airport', 'id'), 'a1aa653e-8cdb-59e8-8e48-dd5b489c55ab');
    assert.notStrictEqual(stableColumnId('airport', 'name'), stableColumnId('island', 'name'));
  });

  it('emits Kart columns for a flat model, skipping bbox and marking id as primary key', () => {
    const model = {
      kind: 'Model',
      properties: new Map([
        mockProp('id', { kind: 'Scalar', name: 'string' }),
        mockProp('created_at', { kind: 'Scalar', name: 'utcDateTime' }),
        mockProp('t50_fid', {
          kind: 'Union',
          variants: new Map([
            ['a', { type: { kind: 'Scalar', name: 'uint32' } }],
            ['b', { type: { kind: 'Intrinsic', name: 'null' } }],
          ]),
        }),
        mockProp('height', { kind: 'Scalar', name: 'float64' }),
        mockProp('active', { kind: 'Scalar', name: 'boolean' }),
        mockProp('type', { kind: 'String', value: 'airport' }),
        mockProp('geometry', { kind: 'Intrinsic', name: 'unknown' }),
        mockProp('bbox', { kind: 'Model', name: 'BBox', properties: new Map() }),
      ]),
    };

    const cols = getKartColumns('airport', model as never);
    assert.deepStrictEqual(cols, [
      { id: stableColumnId('airport', 'id'), name: 'id', dataType: 'text', primaryKeyIndex: 0 },
      { id: stableColumnId('airport', 'created_at'), name: 'created_at', dataType: 'text' },
      { id: stableColumnId('airport', 't50_fid'), name: 't50_fid', dataType: 'integer', size: 32 },
      { id: stableColumnId('airport', 'height'), name: 'height', dataType: 'float', size: 64 },
      { id: stableColumnId('airport', 'active'), name: 'active', dataType: 'boolean' },
      { id: stableColumnId('airport', 'type'), name: 'type', dataType: 'text' },
      { id: stableColumnId('airport', 'geometry'), name: 'geometry', dataType: 'geometry' },
    ]);
  });

  it('merges union variant columns so non-null variant types win over null', () => {
    const variantA = {
      kind: 'Model',
      properties: new Map([
        mockProp('id', { kind: 'Scalar', name: 'string' }),
        mockProp('orientation', { kind: 'Intrinsic', name: 'null' }),
        mockProp('always_null', { kind: 'Intrinsic', name: 'null' }),
      ]),
    };
    const variantB = {
      kind: 'Model',
      properties: new Map([
        mockProp('id', { kind: 'Scalar', name: 'string' }),
        mockProp('orientation', { kind: 'Scalar', name: 'int32' }),
        mockProp('always_null', { kind: 'Intrinsic', name: 'null' }),
      ]),
    };
    const union = {
      kind: 'Union',
      variants: new Map([
        ['a', { type: variantA }],
        ['b', { type: variantB }],
      ]),
    };

    const cols = getKartColumnsForUnion('land_cover_point', union as never);
    assert.deepStrictEqual(cols, [
      { id: stableColumnId('land_cover_point', 'id'), name: 'id', dataType: 'text', primaryKeyIndex: 0 },
      { id: stableColumnId('land_cover_point', 'orientation'), name: 'orientation', dataType: 'integer', size: 32 },
      { id: stableColumnId('land_cover_point', 'always_null'), name: 'always_null', dataType: 'text' },
    ]);
  });
});
