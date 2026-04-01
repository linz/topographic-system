import assert from 'node:assert';
import { afterEach, before, beforeEach, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { readFileWithRetry } from '../action.diff.ts';

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
    for (const f of [testFile, testEmptyFile, testNoFile]) {
      try {
        await fsa.delete(f);
      } catch {
        // ignore if not found
      }
    }
    await fsa.write(testFile, 'hello world');
    await fsa.write(testEmptyFile, '');
  });

  afterEach(async () => {
    for (const f of [testFile, testEmptyFile, testNoFile]) {
      try {
        await fsa.delete(f);
      } catch {
        // ignore if not found
      }
    }
  });

  it('should read a file that exists', async () => {
    const content = await readFileWithRetry(testFile, 3, 100);
    assert.equal(content.toString(), 'hello world');
  });

  it('should read an empty file', async () => {
    const content = await readFileWithRetry(testEmptyFile, 3, 100);
    assert.equal(content.toString(), '');
  });

  it('should fail when file does not exist', async () => {
    await assert.rejects(() => readFileWithRetry(testNoFile, 3, 100));
  });

  it('should read a file that appears while retrying', async () => {
    // Write the file after a short delay, while readFileWithRetry is retrying
    setTimeout(() => fsa.write(testNoFile, 'appeared'), 150);

    const content = await readFileWithRetry(testNoFile, 5, 100);
    assert.equal(content.toString(), 'appeared');
  });
  it('should read a file that exists', async () => {
    const content = await readFileWithRetry(testFile, 3, 100);
    assert.equal(Buffer.isBuffer(content), true);
    assert.equal(content.toString(), 'hello world');
  });
  it('should include the file path in the error message', async () => {
    await assert.rejects(
      () => readFileWithRetry(testNoFile, 3, 100),
      (err: Error) => {
        assert.match(err.message, /nofile\.txt/);
        assert.match(err.message, /3 retries/);
        return true;
      },
    );
  });

  it('should respect the number of retries', async () => {
    // With 1 retry and no file, should fail fast
    const start = Date.now();
    await assert.rejects(() => readFileWithRetry(testNoFile, 1, 50));
    const elapsed = Date.now() - start;
    // 1 retry = one delay of 50ms (50 * 2^0), should be quick
    assert(elapsed < 200, `Expected fast failure, took ${elapsed}ms`);
  });
});
