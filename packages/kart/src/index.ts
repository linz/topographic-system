import { run, subcommands } from 'cmd-ts';
import { ProcessOutput } from 'zx';

import { CloneCommand } from './cli/action.clone.ts';
import { ContourWithLandcoverCommand } from './cli/action.contour.landcover.ts';
import { DiffCommand } from './cli/action.diff.ts';
import { ExportCommand } from './cli/action.export.ts';
// import { FlowCommand } from './cli/action.flow.ts';
import { CommentCommand } from './cli/action.pr.comment.ts';
import { ParquetCommand } from './cli/action.to.parquet.ts';
import { ValidateCommand } from './cli/action.validate.ts';
import { VersionCommand } from './cli/action.version.ts';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    // 'kart-flow': FlowCommand,
    clone: CloneCommand,
    diff: DiffCommand,
    export: ExportCommand,
    'to-parquet': ParquetCommand,
    'pr-comment': CommentCommand,
    validate: ValidateCommand,
    version: VersionCommand,
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

  setTimeout(() => process.exit(1), 10);
});
