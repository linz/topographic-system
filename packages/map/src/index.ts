import { run, subcommands } from 'cmd-ts';

import { deployCommand } from './cli/action.deploy.ts';
import { produceCoverCommand } from './cli/action.produce.cover.ts';
import { ProduceCommand } from './cli/action.produce.ts';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    produce: ProduceCommand,
    'produce-cover': produceCoverCommand,
    deploy: deployCommand,
  },
});

run(Cli, process.argv.slice(2)).catch((err) => {
  console.log(err);

  // Give the logger some time to flush before exiting
  setTimeout(() => process.exit(1), 25);
});
