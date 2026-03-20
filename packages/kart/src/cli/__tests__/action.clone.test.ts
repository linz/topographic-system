import assert from 'node:assert';
import { describe, it } from 'node:test';

import { stringToUrlFolder } from '@linzjs/topographic-system-shared/src/url.ts';

import { buildCloneContext } from '../action.clone.ts';

describe('buildCloneContext', () => {
  it('should resolve a bare org/repo against github.com', () => {
    const ctx = buildCloneContext({ repository: 'linz/topographic-data' });

    assert.strictEqual(ctx.repoUrl.href, 'https://github.com/linz/topographic-data');
    assert.strictEqual(ctx.credentialUrl.href, 'https://github.com/linz/topographic-data');
  });

  it('should default ref to master', () => {
    const ctx = buildCloneContext({ repository: 'linz/topographic-data' });

    assert.strictEqual(ctx.ref, 'master');
  });

  it('should use the provided ref', () => {
    const ctx = buildCloneContext({ repository: 'linz/topographic-data', ref: 'abc123' });

    assert.strictEqual(ctx.ref, 'abc123');
  });

  it('should default output to a "repo" folder URL', () => {
    const ctx = buildCloneContext({ repository: 'linz/topographic-data' });

    assert.ok(ctx.target.href.endsWith('/repo/'), `expected target to end with /repo/, got: ${ctx.target.href}`);
    assert.strictEqual(ctx.target.protocol, 'file:');
  });

  it('should use a custom output when provided', () => {
    const output = stringToUrlFolder('/tmp/my-output');
    const ctx = buildCloneContext({ repository: 'linz/topographic-data', output });

    assert.strictEqual(ctx.target.href, output.href);
  });

  it('should inject GITHUB_TOKEN as x-access-token credentials', () => {
    const ctx = buildCloneContext({ repository: 'linz/topographic-data' }, 'ghp_testtoken123');

    assert.strictEqual(ctx.credentialUrl.username, 'x-access-token');
    assert.strictEqual(ctx.credentialUrl.password, 'ghp_testtoken123');
    assert.strictEqual(ctx.credentialUrl.host, 'github.com');
    assert.strictEqual(ctx.credentialUrl.pathname, '/linz/topographic-data');
  });

  it('should not include credentials when token is undefined', () => {
    const ctx = buildCloneContext({ repository: 'linz/topographic-data' });

    assert.strictEqual(ctx.credentialUrl.username, '');
    assert.strictEqual(ctx.credentialUrl.password, '');
    assert.strictEqual(ctx.credentialUrl.href, ctx.repoUrl.href);
  });

  it('should keep repoUrl clean even when credentials are injected', () => {
    const ctx = buildCloneContext({ repository: 'linz/topographic-data' }, 'ghp_secret');

    // repoUrl must never contain credentials (safe for logging)
    assert.strictEqual(ctx.repoUrl.username, '');
    assert.strictEqual(ctx.repoUrl.password, '');
    assert.ok(!ctx.repoUrl.href.includes('ghp_secret'));
  });

  it('should throw for non-github.com hosts', () => {
    assert.throws(
      () => buildCloneContext({ repository: 'https://gitlab.com/linz/topographic-data' }),
      (err: Error) => {
        assert.ok(err.message.includes('Invalid host'), `expected "Invalid host", got: ${err.message}`);
        return true;
      },
    );
  });

  it('should not produce double slashes in the repo URL', () => {
    const ctx = buildCloneContext({ repository: '/linz/topographic-data' });
    assert.ok(
      !ctx.repoUrl.href.includes('//linz'),
      `URL should not have doubled slashes before org, got: ${ctx.repoUrl.href}`,
    );
  });

  it('should handle a full github https URL as repository input', () => {
    const ctx = buildCloneContext({ repository: 'https://github.com/linz/topographic-data' });

    assert.strictEqual(ctx.repoUrl.href, 'https://github.com/linz/topographic-data');
  });
});
