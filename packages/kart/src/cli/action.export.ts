import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { ConcurrentQueue, logger, stringToUrlFolder, UrlFolder, gitContext } from '@linzjs/topographic-system-shared';
import { command, flag, option, optional, restPositionals, string } from 'cmd-ts';
import { $ } from 'zx';

const numParallelExportProcesses = 1;
const Q = new ConcurrentQueue(numParallelExportProcesses);

type KartDiffOutput = Record<string, number>;

export const ExportCommand = command({
  name: 'export',
  description: 'Export a kart repository and fetch a specific commit',
  args: {
    context: option({
      type: UrlFolder,
      long: 'context',
      short: 'C',
      description:
        'Run as if git was started in <path> instead of the current working directory see git -C for more details',
      defaultValue: () => stringToUrlFolder('repo'),
    }),
    output: option({
      type: UrlFolder,
      long: 'output',
      description: 'Optional output directory for export results (default: $TMPDIR/kart/export)',
      defaultValue: () => stringToUrlFolder(path.join(tmpdir(), 'kart', 'export')),
    }),
    ref: option({
      type: optional(string),
      long: 'ref',
      description: 'Commit SHA or branch to export (default: FETCH_HEAD)',
      defaultValue: () => 'FETCH_HEAD',
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
    const ref = args.ref ?? 'FETCH_HEAD';
    logger.info({ ref, datasets: args.datasets }, 'Export:Start');
    const allDatasetsRequested = args.datasets.length === 0;
    let datasets = new Set<string>();
    if (args.changed) {
      logger.info('Export:OnlyChangedDatasets');
      const kartData = await $`kart ${gitContext(args.context)} diff master..${ref} -o json --only-feature-count exact`;
      const diffOutput = JSON.parse(kartData.stdout) as KartDiffOutput;
      datasets = new Set(Object.keys(diffOutput));
    } else {
      logger.info('Export:AllDatasets');
      const kartData = await $`kart ${gitContext(args.context)} data ls`;
      datasets = new Set(kartData.stdout.split('\n').filter(Boolean));
    }
    logger.info({ datasets: [...datasets] }, 'Export:DatasetsListed');
    const exportDir = fileURLToPath(args.output);
    await $`mkdir -p ${exportDir}`; // kart will fail with unclear error if this doesn't exist
    const datasetsToProcess = allDatasetsRequested
      ? [...datasets]
      : [...new Set(args.datasets)].filter((dataset) => datasets.has(dataset));
    logger.info({ numParallelExportProcesses, datasetsToProcess }, 'Export:DatasetsToProcess');
    datasetsToProcess.map((dataset) =>
      Q.push(
        () =>
          $`kart ${gitContext(args.context)} export ${dataset} --ref ${ref} ${path.join(exportDir, `${dataset}.gpkg`)}`,
      ),
    );
    await Q.join().catch((err: unknown) => {
      logger.fatal({ err }, 'Export:Error');
      throw err;
    });
    logger.info('Export:Completed');
  },
});
