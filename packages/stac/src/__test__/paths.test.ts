import assert from 'node:assert';
import { describe, it } from 'node:test';
import { URL } from 'url';

import { getRelativePath } from '../stac.paths.ts';

describe('makeRelative', () => {
  it('should create relative paths', () => {
    const sourcefile = new URL('https://foo.com/foo/bar/collection.json');
    const sourceDir = new URL('.', sourcefile);
    const target = new URL('https://foo.com/bar/collection.json');

    const pathFromDir = getRelativePath(target, sourceDir);

    assert.equal(pathFromDir, '../../bar/collection.json');
    assert.equal(new URL(pathFromDir, sourceDir).href, target.href);

    const pathFromFile = getRelativePath(target, sourcefile);
    assert.equal(pathFromFile, '../../bar/collection.json');
    assert.equal(new URL(pathFromFile, sourcefile).href, target.href);
  });

  it('should not make relative if protocol hosts or other parts differ', () => {
    assert.equal(getRelativePath(new URL('s3://foo/a.txt'), new URL('s3://foo/b.txt')), './a.txt')
    assert.equal(getRelativePath(new URL('file:///foo/a.txt'), new URL('https://bar/b.txt')), 'file:///foo/a.txt');
    assert.equal(getRelativePath(new URL('s3://foo/a.txt'), new URL('https://bar/b.txt')), 's3://foo/a.txt');
    assert.equal(getRelativePath(new URL('https://foo/a.txt'), new URL('https://bar/b.txt')), 'https://foo/a.txt');

    assert.equal(
      getRelativePath(new URL('https://foo:8081/a.txt'), new URL('https://foo:444/b.txt')),
      'https://foo:8081/a.txt',
    );
    assert.equal(
      getRelativePath(new URL('https://foo@foo/a.txt'), new URL('https://foo:444/b.txt')),
      'https://foo@foo/a.txt',
    );
  });
});
