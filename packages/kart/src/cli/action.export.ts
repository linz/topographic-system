import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { command, option, optional, restPositionals, string } from 'cmd-ts';
import os from 'os';
import { $ } from 'zx';

const Q = new ConcurrentQueue(os.cpus().length);
export const exportCommand = command({
  name: 'export',
  description: 'Export a kart repository and fetch a specific commit',
  args: {
    ref: option({
      type: optional(string),
      long: 'ref',
      description: 'Commit SHA or branch to export (default: HEAD)',
      defaultValue: () => 'HEAD',
    }),
    datasets: restPositionals({
      type: string,
      description: 'List of datasets to export (default: all datasets)',
    }),
  },
  async handler(args) {
    delete $.env['GITHUB_ACTION_REPOSITORY'];
    delete $.env['GITHUB_ACTION_REF'];
    delete $.env['GITHUB_WORKFLOW_REF'];

    logger.info({ ref: args.ref, datasets: args.datasets }, 'Export:Start');
    const allDatasetsRequested = args.datasets.length === 0;
    const kartData = await $`kart -C repo data ls`;
    const datasets = new Set(kartData.stdout.split('\n').filter(Boolean));
    // Ensure the export directory exists before exporting
    await $`mkdir -p ./export`;
    const datasetsToProcess = allDatasetsRequested
      ? [...datasets]
      : [...new Set(args.datasets)].filter((dataset) => datasets.has(dataset));
    logger.info({ datasetsToProcess }, 'Export:DatasetsToProcess');
    datasetsToProcess.map((dataset) =>
      Q.push(() => $`kart -C repo export ${dataset} --ref ${args.ref} ./export/${dataset}.gpkg`),
    );
    await Q.join().catch((err: unknown) => {
      logger.fatal({ err }, 'Export:Error');
      throw err;
    });
    logger.info('Export:Completed');
  },
});
