import assert from 'node:assert';
import { afterEach, before, beforeEach, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import {
  readFileWithRetry,
  truncateDiffLines,
  sumFeatureCounts,
  MaxDiffLines,
  MaxFeatureCount,
  MaxGeoJsonLength,
  buildMarkdownSummary,
  buildTooLargeSummary,
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

describe('sumFeatureCounts', () => {
  it('should return 0 for an empty object', () => {
    assert.equal(sumFeatureCounts({}), 0);
  });

  it('should sum per-dataset counts', () => {
    assert.equal(sumFeatureCounts({ roads: 4, rivers: 1, buildings: 7 }), 12);
  });

  it('should ignore non-numeric values', () => {
    assert.equal(sumFeatureCounts({ roads: 4, bogus: undefined as unknown as number }), 4);
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

// Build matching feature-count and geojson maps from a { dataset: featureCount } spec, so the
// counts buildMarkdownSummary reads line up with the geojson it embeds.
function datasetFixture(
  spec: Record<string, number>,
  padding = 0,
): { counts: Record<string, number>; geojson: Record<string, string> } {
  const counts: Record<string, number> = {};
  const geojson: Record<string, string> = {};
  for (const [name, n] of Object.entries(spec)) {
    counts[name] = n;
    geojson[name] = makeGeoJson(n, padding);
  }
  return { counts, geojson };
}

describe('buildMarkdownSummary', () => {
  it('should return a valid summary for minimal input', () => {
    const result = buildMarkdownSummary({}, '', '', {});
    assert.equal(result.includes('# Changes Summary'), true);
    assert.equal(result.includes('**Total Features Changed**: 0'), true);
    assert.equal(result.includes('**Datasets Affected**: 0'), true);
  });

  it('should truncate git diff to max lines', () => {
    const diff = Array.from({ length: 100 }, (_, i) => `line ${i}`).join('\n');
    const result = buildMarkdownSummary({}, '', diff, {});
    assert.equal(result.includes(`... truncated (showing ${MaxDiffLines} of 100 lines)`), true);
  });

  it('should truncate text diff to max lines', () => {
    const diff = Array.from({ length: 100 }, (_, i) => `line ${i}`).join('\n');
    const result = buildMarkdownSummary({}, diff, '', {});
    assert.equal(result.includes(`... truncated (showing ${MaxDiffLines} of 100 lines)`), true);
  });

  it('should not show truncation notice when diffs are within limits', () => {
    const shortDiff = 'line1\nline2\nline3';
    const result = buildMarkdownSummary({}, shortDiff, shortDiff, {});
    assert.equal(result.includes('truncated'), false);
  });

  it('should include geojson and per-dataset sections when under size limit', () => {
    const { counts, geojson } = datasetFixture({ 'my-layer': 2 });
    const result = buildMarkdownSummary(counts, '', '', geojson);
    assert.equal(result.includes('## Feature Changes Preview'), true);
    assert.equal(result.includes('```geojson'), true);
    assert.equal(result.includes('**my-layer**: 2 features changed'), true);
  });

  it('should show too-large message and hide per-dataset sections when geojson exceeds limit', () => {
    const bigLayer = makeGeoJson(50, 1000);
    const combined = JSON.stringify({ type: 'FeatureCollection', features: JSON.parse(bigLayer).features }, null, 2);
    // Confirm the test data actually exceeds the limit
    assert(combined.length > MaxGeoJsonLength, 'Test data should exceed MaxGeoJsonLength');

    const result = buildMarkdownSummary({ 'big-layer': 50 }, '', '', { 'big-layer': bigLayer });
    assert.equal(result.includes('GeoJSON too large to display'), true);
    assert.equal(result.includes('Changes by Dataset'), false);
  });

  it('should skip datasets with zero features', () => {
    const { counts, geojson } = datasetFixture({ 'empty-layer': 0, 'real-layer': 3 });
    const result = buildMarkdownSummary(counts, '', '', geojson);
    assert.equal(result.includes('**empty-layer**'), false);
    assert.equal(result.includes('**real-layer**: 3 features changed'), true);
    assert.equal(result.includes('**Datasets Affected**: 1'), true);
  });

  it('should show correct counts in header', () => {
    const { counts, geojson } = datasetFixture({ layer1: 2, layer2: 3 });
    const result = buildMarkdownSummary(counts, '', '', geojson);
    assert.equal(result.includes('**Total Features Changed**: 5'), true);
    assert.equal(result.includes('**Datasets Affected**: 2'), true);
  });

  it('should report correct git diff total line count', () => {
    const diff = Array.from({ length: 100 }, (_, i) => `line ${i}`).join('\n');
    const result = buildMarkdownSummary({}, '', diff, {});
    assert.equal(result.includes('**Git Diff Lines**: 100'), true);
  });

  it('should not include geojson section when there are no features', () => {
    const result = buildMarkdownSummary({}, 'some diff', 'some git diff', {});
    assert.equal(result.includes('Feature Changes Preview'), false);
    assert.equal(result.includes('```geojson'), false);
  });

  it('should stay under the GitHub comment size limit at the embed boundary', () => {
    // GitHub rejects PR comment bodies over 65536 chars (MAX_COMMENT_SIZE in the shared GithubApi).
    // MaxGeoJsonLength must be low enough that even the largest summary fits even if the full summary renders.
    const GithubCommentLimit = 65_536;

    const geojson: Record<string, string> = {};
    for (let i = 0; i < 1000; i++) {
      const candidate = { ...geojson, [`layer-${i}`]: makeGeoJson(5, 200) };
      const combined = JSON.stringify(
        { type: 'FeatureCollection', features: Object.values(candidate).flatMap((s) => JSON.parse(s).features) },
        null,
        2,
      );
      if (combined.length > MaxGeoJsonLength) break;
      geojson[`layer-${i}`] = makeGeoJson(5, 200);
    }
    const counts = Object.fromEntries(Object.keys(geojson).map((k) => [k, 5]));

    const hugeDiff = Array.from({ length: 200 }, (_, n) => 'x'.repeat(300) + n).join('\n');
    const result = buildMarkdownSummary(counts, hugeDiff, hugeDiff, geojson);

    assert(
      result.length < GithubCommentLimit,
      `summary length ${result.length} should be under GitHub's ${GithubCommentLimit} limit`,
    );
  });

  it('should show each layer with only its own changed feature count', () => {
    const { counts, geojson } = datasetFixture({ roads: 4, rivers: 1, buildings: 7 });
    const result = buildMarkdownSummary(counts, '', '', geojson);

    assert.equal(result.includes('**Total Features Changed**: 12'), true);
    assert.equal(result.includes('**Datasets Affected**: 3'), true);

    assert.equal(result.includes('**roads**: 4 features changed'), true);
    assert.equal(result.includes('**rivers**: 1 features changed'), true);
    assert.equal(result.includes('**buildings**: 7 features changed'), true);
  });
});

describe('buildTooLargeSummary', () => {
  it('should report the total and per-dataset counts', () => {
    const result = buildTooLargeSummary({ roads: 4, rivers: 1, buildings: 7 });
    assert.equal(result.includes('**Total Features Changed**: 12'), true);
    assert.equal(result.includes('**Datasets Affected**: 3'), true);
    assert.equal(result.includes(`Too many features changed (${MaxFeatureCount} limit)`), true);
    assert.equal(result.includes('### Changes by Dataset'), true);
    assert.equal(result.includes('- **roads**: 4 features changed'), true);
    assert.equal(result.includes('- **rivers**: 1 features changed'), true);
  });

  it('should sort datasets by descending count', () => {
    const result = buildTooLargeSummary({ rivers: 1, buildings: 7, roads: 4 });
    const order = ['buildings', 'roads', 'rivers'].map((d) => result.indexOf(`**${d}**`));
    assert.deepEqual(
      order,
      [...order].sort((a, b) => a - b),
    );
  });

  it('should exclude datasets with zero changes', () => {
    const result = buildTooLargeSummary({ roads: 5, empty: 0 });
    assert.equal(result.includes('**Datasets Affected**: 1'), true);
    assert.equal(result.includes('**empty**'), false);
  });

  it('should omit the per-dataset section when there are no changes', () => {
    const result = buildTooLargeSummary({});
    assert.equal(result.includes('**Total Features Changed**: 0'), true);
    assert.equal(result.includes('**Datasets Affected**: 0'), true);
    assert.equal(result.includes('### Changes by Dataset'), false);
  });

  it('should not emit empty diff code blocks', () => {
    const result = buildTooLargeSummary({ roads: 5 });
    assert.equal(result.includes('## Kart Diff'), false);
    assert.equal(result.includes('## Git Diff'), false);
  });
});
