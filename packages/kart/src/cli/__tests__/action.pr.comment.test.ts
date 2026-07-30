import assert from 'node:assert';
import { before, beforeEach, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { CommentCommand } from '../action.pr.comment.ts';

describe('pr-comment skip behaviour', () => {
  const mem = new FsMemory();

  before(() => {
    fsa.register('memory://', mem);
  });

  beforeEach(() => {
    mem.files.clear();
  });

  // When there is no summary the handler must return before touching the GitHub API, so these
  // assertions double as proof that no comment is posted.
  it('should skip when the summary file is missing', async () => {
    const bodyFile = new URL('memory:///tmp/missing.md');
    await assert.doesNotReject(() => CommentCommand.handler({ pr: undefined, repo: undefined, bodyFile }));
  });

  it('should skip when the summary file is empty or whitespace', async () => {
    const bodyFile = new URL('memory:///tmp/empty.md');
    await fsa.write(bodyFile, '   \n');
    await assert.doesNotReject(() => CommentCommand.handler({ pr: undefined, repo: undefined, bodyFile }));
  });
});
