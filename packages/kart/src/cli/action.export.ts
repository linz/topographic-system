import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { command, flag, option, optional, restPositionals, string } from 'cmd-ts';
import os from 'os';
import { $ } from 'zx';

const Q = new ConcurrentQueue(os.cpus().length);

type KartDiffOutput = Record<string, number>;

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
    changed: flag({
      long: 'changed-datasets-only',
      description: 'Export only datasets changed compared to master (default: false)',
      defaultValue: () => false,
    }),
    datasets: restPositionals({
      type: string,
      description: 'List of datasets to export (default: all, or all changed datasets)',
    }),
  },
  async handler(args) {
    delete $.env['GITHUB_ACTION_REPOSITORY'];
    delete $.env['GITHUB_ACTION_REF'];
    delete $.env['GITHUB_WORKFLOW_REF'];

    logger.info({ ref: args.ref, datasets: args.datasets }, 'Export:Start');
    const allDatasetsRequested = args.datasets.length === 0;
    let datasets = new Set<string>();
    if (args.changed) {
      logger.info('Export:Listing changed datasets only');
      const kartData = await $`kart -C repo diff master..FETCH_HEAD --only-feature-count exact --output-format=json`;
      const diffOutput = JSON.parse(kartData.stdout) as KartDiffOutput;
      datasets = new Set(Object.keys(diffOutput));
    } else {
      logger.info('Export:Listing all datasets');
      const kartData = await $`kart -C repo data ls`;
      datasets = new Set(kartData.stdout.split('\n').filter(Boolean));
    }
    logger.info({ datasets: [...datasets] }, 'Export:DatasetsListed');
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
