import { isGitHubActions } from '@topographic-system/shared/src/env.ts';
import { run, subcommands } from 'cmd-ts';

import { deployCommand } from './cli/action.deploy.ts';
import { listMapSheetsCommand } from './cli/action.list.mapsheet.ts';
import { ProduceCommand } from './cli/action.produce.ts';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    produce: ProduceCommand,
    'list-mapsheets': listMapSheetsCommand,
    deploy: deployCommand,
  },
});

run(Cli, process.argv.slice(2)).catch((err) => {
  console.log(err);

  // Only force failure in GitHub Actions
  if (isGitHubActions) {
    process.exit(1);
  }
});
