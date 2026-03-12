import { CliInfo, createOtelSdk, trace } from '@linzjs/topographic-system-shared';
import { monitor } from '@linzjs/topographic-system-shared';
import { run, subcommands } from 'cmd-ts';

import { deployCommand } from './cli/action.deploy.ts';
import { produceCoverCommand } from './cli/action.produce.cover.ts';
import { ProduceCommand } from './cli/action.produce.ts';

const cmds = {
  produce: ProduceCommand,
  'produce-cover': produceCoverCommand,
  deploy: deployCommand,
};

const Cli = subcommands({
  name: 'topographic-map',
  description: 'Deploy and export topographic maps',
  cmds,
});

for (const cmd of Object.values(cmds)) monitor(cmd, 'handler', { name: `cli.map.${cmd.name}` });

CliInfo.package = '@linzjs/topographic-system-map';
const sdk = createOtelSdk(CliInfo.package);

sdk?.start();

let exitCode = 0;
void trace('cli', async (span) => {
  await run(Cli, process.argv.slice(2))
    .catch(async (err) => {
      console.log(err);
      span.recordException(err);
      exitCode = 1;
    })
    .finally(async () => {
      span.end();
      await sdk?.shutdown();
      process.exit(exitCode);
    });
});
