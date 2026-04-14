import assert from 'node:assert';
import { rm } from 'node:fs/promises';
import { beforeEach, describe, it } from 'node:test';

import { fsa } from '@chunkd/fs';

import { recursiveFileSearch } from '../fs.util.ts';

describe('recursiveFileSearch', () => {
  const testBasePath = '/tmp/test/fs-util-test/';
  const testBaseUrl = fsa.toUrl(testBasePath);

  beforeEach(async () => {
    await rm(testBasePath, { recursive: true, force: true });
  });

  it('should return empty array for non-existent path', async () => {
    const result = await recursiveFileSearch(new URL('non-existent', testBaseUrl));
    assert.deepStrictEqual(result, []);
  });

  it('should return single file when path is a file', async () => {
    const filePath = new URL('path/to/test-file.txt', testBaseUrl);
    await fsa.write(filePath, 'content');

    const result = await recursiveFileSearch(filePath);
    assert.deepStrictEqual(result, [filePath]);
  });

  it('should return single file when path is a file and extension matches', async () => {
    const filePath = new URL('test-file.txt', testBaseUrl);
    await fsa.write(filePath, 'content');

    const result = await recursiveFileSearch(filePath, '.txt');
    assert.deepStrictEqual(result, [filePath]);
  });

  it('should return empty array when path is a file and extension does not match', async () => {
    const filePath = new URL('test-file.txt', testBaseUrl);
    await fsa.write(filePath, 'content');

    const result = await recursiveFileSearch(filePath, '.json');
    assert.deepStrictEqual(result, []);
  });

  it('should return all files in directory recursively', async () => {
    const dirPath = new URL('dir/', testBaseUrl);
    await fsa.write(new URL('dir/file1.txt', testBaseUrl), 'content1');
    await fsa.write(new URL('dir/file2.txt', testBaseUrl), 'content2');
    await fsa.write(new URL('dir/sub/file3.txt', testBaseUrl), 'content3');

    // FsMemory reports directories with isDirectory flag when files exist under the path
    const result = await recursiveFileSearch(dirPath);
    assert.ok(result.length === 3);
  });

  it('should filter files by extension in directory', async () => {
    const dirPath = new URL('dir2/', testBaseUrl);
    await fsa.write(new URL('dir2/file1.txt', testBaseUrl), 'content1');
    await fsa.write(new URL('dir2/file2.json', testBaseUrl), 'content2');
    await fsa.write(new URL('dir2/sub/file3.txt', testBaseUrl), 'content3');

    const result = await recursiveFileSearch(dirPath, '.txt');

    assert.ok(result.length === 2);
    assert.ok(result.every((url) => url.href.endsWith('.txt')));
  });

  it('should return empty array when directory has no files matching extension', async () => {
    const dirPath = new URL('dir3/', testBaseUrl);
    await fsa.write(new URL('dir3/file1.txt', testBaseUrl), 'content1');
    await fsa.write(new URL('dir3/file2.txt', testBaseUrl), 'content2');

    const result = await recursiveFileSearch(dirPath, '.json');
    assert.deepStrictEqual(result, []);
  });
});
