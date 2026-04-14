import assert from 'node:assert';
import { afterEach, before, beforeEach, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import {
  readFileWithRetry,
  truncateDiffLines,
  MaxDiffLines,
  MaxGeoJsonLength,
  buildMarkdownSummary,
} from '../action.diff.ts';

describe('readWithRetry', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  const testBasePath = fsa.toUrl('memory:///tmp/test/diff-test/');
  const testFile = new URL('file.txt', testBasePath);
  const testEmptyFile = new URL('emptyfile.txt', testBasePath);
  const testNoFile = new URL('nofile.txt', testBasePath);

  beforeEach(async () => {
    mem.files.clear();
    await fsa.write(testFile, 'hello world');
    await fsa.write(testEmptyFile, '');
  });

  afterEach(async () => {
    mem.files.clear();
  });

  it('should read a file that exists', async () => {
    const content = await readFileWithRetry(testFile, 2, 2);
    assert.equal(content.toString(), 'hello world');
  });

  it('should read an empty file', async () => {
    const content = await readFileWithRetry(testEmptyFile, 2, 2);
    assert.equal(content.toString(), '');
  });

  it('should fail when file does not exist', async () => {
    await assert.rejects(() => readFileWithRetry(testNoFile, 2, 2));
  });

  it('should read a file that appears while retrying', async () => {
    // Write the file after a short delay, while readFileWithRetry is retrying
    setTimeout(() => fsa.write(testNoFile, 'appeared'), 5);

    const content = await readFileWithRetry(testNoFile, 5, 2);
    assert.equal(content.toString(), 'appeared');
  });

  it('should read a file that exists', async () => {
    const content = await readFileWithRetry(testFile);
    assert.equal(Buffer.isBuffer(content), true);
    assert.equal(content.toString(), 'hello world');
  });

  it('should include the file path in the error message', async () => {
    await assert.rejects(
      () => readFileWithRetry(testNoFile, 2, 2),
      (err: Error) => {
        assert.equal(err.message, 'Failed to read file memory:///tmp/test/diff-test/nofile.txt after 2 retries');
        return true;
      },
    );
  });

  it('should respect the number of retries', async () => {
    const start = Date.now();
    await assert.rejects(() => readFileWithRetry(testNoFile, 1, 1));
    const elapsed = Date.now() - start;
    // 1 retry = one delay of 1ms should be quick
    assert(elapsed < 3, `Expected fast failure, took ${elapsed}ms`);
  });
});

describe('truncateDiffLines', () => {
  it('should return the original text when under limits', () => {
    const diff = 'line1\nline2\nline3';
    const result = truncateDiffLines(diff, 5, 100);
    assert.equal(result.text, diff);
    assert.equal(result.truncated, false);
    assert.equal(result.totalLines, 3);
  });

  it('should handle an empty string', () => {
    const result = truncateDiffLines('', 5, 100);
    assert.equal(result.text, '');
    assert.equal(result.truncated, false);
    assert.equal(result.totalLines, 1);
  });

  it('should truncate lines beyond the max', () => {
    const diff = 'line1\nline2\nline3';
    const result = truncateDiffLines(diff, 2, 100);
    assert.equal(result.text.split('\n').length, 2);
    assert.equal(result.truncated, true);
    assert.equal(result.totalLines, 3);
  });

  it('should not truncate when exactly at the line limit', () => {
    const diff = 'line1\nline2\nline3';
    const result = truncateDiffLines(diff, 3, 100);
    assert.equal(result.text, diff);
    assert.equal(result.truncated, false);
    assert.equal(result.totalLines, 3);
  });

  it('should truncate long lines with ellipsis', () => {
    const longLine = 'a'.repeat(10);
    const result = truncateDiffLines(longLine, 10, 5);
    assert.equal(result.text, 'a'.repeat(5) + '…');
    assert.equal(result.truncated, false);
  });

  it('should not truncate lines exactly at the max length', () => {
    const exactLine = 'a'.repeat(5);
    const result = truncateDiffLines(exactLine, 10, 5);
    assert.equal(result.text, exactLine);
  });

  it('should truncate both lines and line length together', () => {
    const diff = `${'a'.repeat(10)}\n${'b'.repeat(10)}\nccc`;
    const result = truncateDiffLines(diff, 2, 5);

    const outputLines = result.text.split('\n');
    assert.equal(outputLines.length, 2);
    assert.equal(result.truncated, true);
    assert.equal(result.totalLines, 3);
    assert.equal(outputLines[0], 'a'.repeat(5) + '…');
    assert.equal(outputLines[1], 'b'.repeat(5) + '…');
  });
});

function makeGeoJson(featureCount: number, padding = 0): string {
  const features = Array.from({ length: featureCount }, (_, i) => ({
    type: 'Feature',
    properties: { id: i, data: 'x'.repeat(padding) },
    geometry: { type: 'Point', coordinates: [174.7, -41.3] },
  }));
  return JSON.stringify({ type: 'FeatureCollection', features });
}

describe('buildMarkdownSummary', () => {
  it('should return a valid summary for minimal input', () => {
    const result = buildMarkdownSummary(0, '', '', {});
    assert.equal(result.includes('# Changes Summary'), true);
    assert.equal(result.includes('**Total Features Changed**: 0'), true);
    assert.equal(result.includes('**Datasets Affected**: 0'), true);
  });

  it('should truncate git diff to max lines', () => {
    const diff = Array.from({ length: 100 }, (_, i) => `line ${i}`).join('\n');
    const result = buildMarkdownSummary(0, '', diff, {});
    assert.equal(result.includes(`... truncated (showing ${MaxDiffLines} of 100 lines)`), true);
  });

  it('should truncate text diff to max lines', () => {
    const diff = Array.from({ length: 100 }, (_, i) => `line ${i}`).join('\n');
    const result = buildMarkdownSummary(0, diff, '', {});
    assert.equal(result.includes(`... truncated (showing ${MaxDiffLines} of 100 lines)`), true);
  });

  it('should not show truncation notice when diffs are within limits', () => {
    const shortDiff = 'line1\nline2\nline3';
    const result = buildMarkdownSummary(0, shortDiff, shortDiff, {});
    assert.equal(result.includes('truncated'), false);
  });

  it('should include geojson and per-dataset sections when under size limit', () => {
    const datasets = { 'my-layer': makeGeoJson(2) };
    const result = buildMarkdownSummary(2, '', '', datasets);
    assert.equal(result.includes('## Feature Changes Preview'), true);
    assert.equal(result.includes('```geojson'), true);
    assert.equal(result.includes('**my-layer**: 2 features changed'), true);
  });

  it('should show too-large message and hide per-dataset sections when geojson exceeds limit', () => {
    const datasets = { 'big-layer': makeGeoJson(50, 1000) };
    const combined = JSON.stringify(
      {
        type: 'FeatureCollection',
        features: JSON.parse(datasets['big-layer']).features,
      },
      null,
      2,
    );
    // Confirm the test data actually exceeds the limit
    assert(combined.length > MaxGeoJsonLength, 'Test data should exceed MaxGeoJsonLength');

    const result = buildMarkdownSummary(50, '', '', datasets);
    assert.equal(result.includes('GeoJSON too large to display'), true);
    assert.equal(result.includes('Changes by Dataset'), false);
  });

  it('should skip datasets with zero features', () => {
    const datasets = {
      'empty-layer': makeGeoJson(0),
      'real-layer': makeGeoJson(3),
    };
    const result = buildMarkdownSummary(3, '', '', datasets);
    assert.equal(result.includes('**empty-layer**'), false);
    assert.equal(result.includes('**real-layer**: 3 features changed'), true);
  });

  it('should show correct counts in header', () => {
    const datasets = { layer1: makeGeoJson(2), layer2: makeGeoJson(3) };
    const result = buildMarkdownSummary(5, '', '', datasets);
    assert.equal(result.includes('**Total Features Changed**: 5'), true);
    assert.equal(result.includes('**Datasets Affected**: 2'), true);
  });

  it('should report correct git diff total line count', () => {
    const diff = Array.from({ length: 100 }, (_, i) => `line ${i}`).join('\n');
    const result = buildMarkdownSummary(0, '', diff, {});
    assert.equal(result.includes('**Git Diff Lines**: 100'), true);
  });

  it('should not include geojson section when there are no features', () => {
    const result = buildMarkdownSummary(0, 'some diff', 'some git diff', {});
    assert.equal(result.includes('Feature Changes Preview'), false);
    assert.equal(result.includes('```geojson'), false);
  });

  it('should show each layer with only its own changed feature count', () => {
    const datasets = {
      roads: makeGeoJson(4),
      rivers: makeGeoJson(1),
      buildings: makeGeoJson(7),
    };
    const result = buildMarkdownSummary(12, '', '', datasets);

    assert.equal(result.includes('**Total Features Changed**: 12'), true);
    assert.equal(result.includes('**Datasets Affected**: 3'), true);

    assert.equal(result.includes('**roads**: 4 features changed'), true);
    assert.equal(result.includes('**rivers**: 1 features changed'), true);
    assert.equal(result.includes('**buildings**: 7 features changed'), true);
  });
});
