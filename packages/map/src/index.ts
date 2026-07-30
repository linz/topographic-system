import { traceAndRun } from '@linzjs/topographic-system-shared';
import { StacPushCommand } from '@linzjs/topographic-system-stac';
import { subcommands } from 'cmd-ts';

import packageJson from '../package.json' with { type: 'json' };
import { DeployCommand } from './cli/action.deploy.ts';
import { ExportCommand } from './cli/action.export.ts';
import { PrepareCommand } from './cli/action.prepare.ts';
import { VersionCommand } from './cli/action.version.ts';
import { VisualDiffCommand } from './cli/action.visual.diff.ts';

const cmds = {
  export: ExportCommand,
  prepare: PrepareCommand,
  deploy: DeployCommand,
  'visual-diff': VisualDiffCommand,
  version: VersionCommand,
  'stac-push': StacPushCommand,

  /** @deprecated 2026-05 */
  produce: ExportCommand,
  /** @deprecated 2026-05 */
  'produce-cover': PrepareCommand,
};

const Cli = subcommands({
  name: 'topographic-map',
  description: 'Deploy and export topographic maps',
  cmds,
});

void traceAndRun(Cli, cmds, packageJson.name);
