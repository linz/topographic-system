import { run, subcommands } from 'cmd-ts';
import { ProcessOutput } from 'zx';

import { cloneCommand } from './cli/action.clone.ts';
import { ContourWithLandcoverCommand } from './cli/action.contour.landcover.ts';
import { diffCommand } from './cli/action.diff.ts';
import { exportCommand } from './cli/action.export.ts';
import { commentCommand } from './cli/action.pr.comment.ts';
import { parquetCommand } from './cli/action.to.parquet.ts';
import { validateCommand } from './cli/action.validate.ts';
import { versionCommand } from './cli/action.version.ts';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    clone: cloneCommand,
    diff: diffCommand,
    export: exportCommand,
    'to-parquet': parquetCommand,
    'pr-comment': commentCommand,
    validate: validateCommand,
    version: versionCommand,
    'contour-with-landcover': ContourWithLandcoverCommand,
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
