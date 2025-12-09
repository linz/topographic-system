import { run, subcommands } from 'cmd-ts';

import { downloadCommand } from './cli/action.download.ts';
import { listMapSheetsCommand } from './cli/action.list.mapsheet.ts';
import { ProduceCommand } from './cli/action.produce.ts';

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    produce: ProduceCommand,
    'list-mapsheets': listMapSheetsCommand,
    download: downloadCommand,
  },
});

run(Cli, process.argv.slice(2)).catch((err) => {
  console.log(err);
});
