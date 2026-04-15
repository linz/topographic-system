import { traceAndRun } from '@linzjs/topographic-system-shared';
import { StacPushCommand } from '@linzjs/topographic-system-stac';
import { subcommands } from 'cmd-ts';

import packageJson from '../package.json' with { type: 'json' };
import { DeployCommand } from './cli/action.deploy.ts';
import { ProduceCoverCommand } from './cli/action.produce.cover.ts';
import { ProduceCommand } from './cli/action.produce.ts';
import { VersionCommand } from './cli/action.version.ts';
import { VisualDiffCommand } from './cli/action.visual.diff.ts';

const cmds = {
  produce: ProduceCommand,
  'produce-cover': ProduceCoverCommand,
  deploy: DeployCommand,
  'visual-diff': VisualDiffCommand,
  version: VersionCommand,
  'stac-push': StacPushCommand,
};

const Cli = subcommands({
  name: 'topographic-map',
  description: 'Deploy and export topographic maps',
  cmds,
});

void traceAndRun(Cli, cmds, packageJson.name);
