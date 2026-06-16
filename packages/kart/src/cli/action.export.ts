import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  logger,
  stringToUrlFolder,
  UrlFolder,
  gitContext,
  qFromArgs,
  qMapAll,
  worker,
} from '@linzjs/topographic-system-shared';
import { command, flag, option, optional, restPositionals, string } from 'cmd-ts';
import { $ } from 'zx';

type KartDiffOutput = Record<string, number>;

/**
 * Select which datasets to export.
 * Based on which datasets were requested, and which exist on the target ref.
 *
 * @param existing Datasets that exist at the ref being exported (`kart data ls <ref>`).
 * @param requested Datasets explicitly requested on the CLI; empty means "all".
 * @param changedKeys Changed datasets from the diff, or undefined to export all existing datasets.
 */
export function selectExportDatasets(existing: Set<string>, requested: string[], changedKeys?: string[]): string[] {
  const eligible = changedKeys ? changedKeys.filter((dataset) => existing.has(dataset)) : [...existing];
  if (requested.length === 0) return eligible;
  const eligibleSet = new Set(eligible);
  return [...new Set(requested)].filter((dataset) => eligibleSet.has(dataset));
}

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
    worker,
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

    const existingList = await $`kart ${gitContext(args.context)} data ls ${ref}`;
    const existing = new Set(existingList.stdout.split('\n').filter(Boolean));
    logger.info({ datasets: [...existing] }, 'Export:DatasetsListed');

    let changedKeys: string[] | undefined;
    if (args.changed) {
      logger.info('Export:OnlyChangedDatasets');
      const kartData = await $`kart ${gitContext(args.context)} diff master..${ref} -o json --only-feature-count exact`;
      changedKeys = Object.keys(JSON.parse(kartData.stdout) as KartDiffOutput);
    } else {
      logger.info('Export:AllDatasets');
    }

    const exportDir = fileURLToPath(args.output);
    await $`mkdir -p ${exportDir}`; // kart will fail with unclear error if this doesn't exist
    const datasetsToProcess = selectExportDatasets(existing, args.datasets, changedKeys);
    logger.info({ worker: args.worker, datasetsToProcess }, 'Export:DatasetsToProcess');

    await qMapAll(
      q,
      datasetsToProcess,
      (dataset) => $`kart ${buildKartExportArgs(dataset, exportDir, ref, args.context)}`,
    );

    logger.info('Export:Completed');
  },
});
