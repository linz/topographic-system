import { isArgo } from '@topographic-system/shared/src/argo.ts';
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

  // Only force failure in GitHub Actions or Argo Workflows, otherwise just log the error for local debugging.
  if (isGitHubActions || isArgo()) {
    process.exit(1);
  }
});
