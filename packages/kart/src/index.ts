import { traceAndRun } from '@linzjs/topographic-system-shared';
import { subcommands } from 'cmd-ts';

import packageJson from '../package.json' with { type: 'json' };
import { cloneCommand } from './cli/action.clone.ts';
import { ContourWithLandcoverCommand } from './cli/action.contour.landcover.ts';
import { diffCommand } from './cli/action.diff.ts';
import { exportCommand } from './cli/action.export.ts';
import { commentCommand } from './cli/action.pr.comment.ts';
import { parquetCommand } from './cli/action.to.parquet.ts';
import { validateCommand } from './cli/action.validate.ts';
import { versionCommand } from './cli/action.version.ts';

const cmds = {
  clone: cloneCommand,
  diff: diffCommand,
  export: exportCommand,
  'to-parquet': parquetCommand,
  'pr-comment': commentCommand,
  validate: validateCommand,
  version: versionCommand,
  'contour-with-landcover': ContourWithLandcoverCommand,
};

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds,
});

void traceAndRun(Cli, cmds, packageJson.name);
