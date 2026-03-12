import { run } from 'cmd-ts';

import { CliInfo } from '../cli.info.ts';
import { monitor } from './instrument.ts';
import { createOtelSdk, trace } from './otel.ts';

export async function traceAndRun(
  cli: Parameters<typeof run>[0],
  cmds: Record<string, { handler: Function; name: string }>,
  packageName: string,
): Promise<void> {
  for (const cmd of Object.values(cmds)) monitor(cmd, 'handler', { name: `cli.${cmd.name}` });

  CliInfo.package = packageName;
  const ret = createOtelSdk(CliInfo.package);

  ret?.sdk.start();

  let exitCode = 0;
  await trace(
    'cli',
    async (span) => {
      await run(cli, process.argv.slice(2))
        .catch(async (err) => {
          console.log(err);
          span.recordException(err);
          exitCode = 1;
        })
        .finally(async () => {
          span.end();
          await ret?.sdk.shutdown();
          process.exit(exitCode);
        });
    },
    ret?.parentContext,
  );
}
