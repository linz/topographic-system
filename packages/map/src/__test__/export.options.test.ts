import assert from 'node:assert';
import { describe, it } from 'node:test';

import { parseFormatOptionString } from '../cli/export.options.ts';

describe('export.options parser', () => {
  it('should parse key=value export options', () => {
    const res = parseFormatOptionString('layout=nztopo50,dpi=600,format=tiff');
    assert.deepEqual(res, {
      layout: 'nztopo50',
      dpi: 600,
      format: 'tiff',
    });
  });

  it('should parse key=value export options with label', () => {
    const res = parseFormatOptionString('layout=nztopo50,dpi=30,format=webp,label=thumbnail');
    assert.deepEqual(res, {
      layout: 'nztopo50',
      dpi: 30,
      format: 'webp',
      label: 'thumbnail',
    });
  });
  it('shoudl default labels if not set', () => {
    const res = parseFormatOptionString('layout=nztopo50,dpi=30,format=webp,role=thumbnail');
    assert.deepEqual(res, {
      layout: 'nztopo50',
      dpi: 30,
      format: 'webp',
      role: 'thumbnail',
      label: 'thumbnail',
    });
  });

  it('should throw on invalid format', () => {
    assert.throws(() => parseFormatOptionString('invalid_format'), /"invalid_format"/);
  });

  it('should throw on missing format in key=value string', () => {
    assert.throws(() => parseFormatOptionString('layout=nztopo50,dpi=600'), /Invalid option:/);
  });
});
