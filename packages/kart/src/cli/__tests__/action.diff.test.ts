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
