import { run, subcommands } from 'cmd-ts';
import {$, ProcessOutput} from 'zx';

import { cloneCommand } from './cli/action.clone.ts';
import { diffCommand } from './cli/action.diff.ts';
import { exportCommand } from './cli/action.export.ts';
import { commentCommand } from './cli/action.pr.comment.ts';
import { versionCommand } from './cli/action.version.ts';

delete $.env['GITHUB_ACTION_REPOSITORY'];
delete $.env['GITHUB_ACTION_REF'];
delete $.env['GITHUB_WORKFLOW_REF'];

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    clone: cloneCommand,
    diff: diffCommand,
    export: exportCommand,
    'pr-comment': commentCommand,
    version: versionCommand,
  },
});

run(Cli, process.argv.slice(2)).catch((err) => {
  // handle zx errors
  if (err instanceof ProcessOutput) {
    console.log(err.stderr);
  } else {
    console.log(err);
  }
});
