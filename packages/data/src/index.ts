import { run, subcommands } from 'cmd-ts';

import { IceContoursCommand } from './cli/action.ice.contours.ts';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    'ice-contours': IceContoursCommand,
  },
});

run(Cli, process.argv.slice(2)).catch((err) => {
  console.log(err);
});
