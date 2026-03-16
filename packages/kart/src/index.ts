import { traceAndRun } from '@linzjs/topographic-system-shared';
import { subcommands } from 'cmd-ts';

import packageJson from '../package.json' with { type: 'json' };
import { CloneCommand } from './cli/action.clone.ts';
import { ContourWithLandcoverCommand } from './cli/action.contour.landcover.ts';
import { DiffCommand } from './cli/action.diff.ts';
import { ExportCommand } from './cli/action.export.ts';
import { CommentCommand } from './cli/action.pr.comment.ts';
import { ParquetCommand } from './cli/action.to.parquet.ts';
import { ValidateCommand } from './cli/action.validate.ts';
import { VersionCommand } from './cli/action.version.ts';

const cmds = {
  clone: CloneCommand,
  diff: DiffCommand,
  export: ExportCommand,
  'to-parquet': ParquetCommand,
  'pr-comment': CommentCommand,
  validate: ValidateCommand,
  version: VersionCommand,
  'contour-with-landcover': ContourWithLandcoverCommand,
};

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds,
});

void traceAndRun(Cli, cmds, packageJson.name);
