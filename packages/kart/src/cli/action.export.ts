import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  logger,
  stringToUrlFolder,
  UrlFolder,
  gitContext,
  concurrency,
  qFromArgs,
} from '@linzjs/topographic-system-shared';
import { command, flag, option, optional, restPositionals, string } from 'cmd-ts';
import { $ } from 'zx';

type KartDiffOutput = Record<string, number>;

export function buildKartExportArgs(dataset: string, outputPath: string, ref: string, context?: URL): string[] {
  const outputFile = path.join(outputPath, `${dataset}.gpkg`);
  return [
    gitContext(context),
    'export',
    ['-lco', 'GEOMETRY_NAME=geometry'],
    dataset,
    ['--ref', ref],
    outputFile,
  ].flat();
}

export const ExportCommand = command({
  name: 'export',
  description: 'Export a kart repository and fetch a specific commit',
  args: {
    concurrency,
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
    const q = qFromArgs(args);
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
    logger.info({ concurrency: args.concurrency, datasetsToProcess }, 'Export:DatasetsToProcess');
    const todo: Promise<unknown>[] = [];
    datasetsToProcess.map((dataset) =>
      todo.push(q(() => $`kart ${buildKartExportArgs(dataset, exportDir, ref, args.context)}`)),
    );
    await Promise.all(todo).catch((err: unknown) => {
      logger.fatal({ err }, 'Export:Error');
      throw err;
    });
    logger.info('Export:Completed');
  },
});
