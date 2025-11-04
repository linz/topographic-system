import { run, subcommands } from 'cmd-ts';

import { ProduceCommand } from './cli/produce';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    produce: ProduceCommand,
  },
});

run(Cli, process.argv.slice(2)).catch((err) => {
  console.log(err);
});
