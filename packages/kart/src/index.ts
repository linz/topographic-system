import { run, subcommands } from 'cmd-ts';
import { ProcessOutput } from 'zx';

import { cloneCommand } from './cli/action.clone.ts';
import { versionCommand } from './cli/action.version.ts';
// import { diffCommand } from './cli/action.diff.ts';
// import { exportCommand } from './cli/action.export.ts';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    clone: cloneCommand,
    version: versionCommand,
    // diff: diffCommand,
    // export: exportCommand,
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
