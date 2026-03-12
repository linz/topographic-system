import { traceAndRun } from '@linzjs/topographic-system-shared';
import { subcommands } from 'cmd-ts';

import packageJson from '../package.json' with { type: 'json' };
import { DeployCommand } from './cli/action.deploy.ts';
import { ProduceCoverCommand } from './cli/action.produce.cover.ts';
import { ProduceCommand } from './cli/action.produce.ts';
import { VisualDiffCommand } from './cli/action.visual.diff.ts';

const cmds = {
  produce: ProduceCommand,
  'produce-cover': ProduceCoverCommand,
  deploy: DeployCommand,
  'visual-diff': VisualDiffCommand,
};

const Cli = subcommands({
  name: 'topographic-map',
  description: 'Deploy and export topographic maps',
  cmds,
});

void traceAndRun(Cli, cmds, packageJson.name);
