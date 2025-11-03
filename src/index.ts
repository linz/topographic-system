import { subcommands } from 'cmd-ts';
import { ProduceCommand } from './cli/produce';

export const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds: {
    produce: ProduceCommand
  },
});