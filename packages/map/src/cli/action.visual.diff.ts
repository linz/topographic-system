import { mkdirSync } from 'fs';

import { fsa } from '@chunkd/fs';
import { logger, qFromArgs, registerFileSystem, Url, worker } from '@linzjs/topographic-system-shared';
import { StacDownloader } from '@linzjs/topographic-system-stac';
import { command, option, optional, string } from 'cmd-ts';

import { pyRunner } from '../python.runner.ts';
import { getQgisCartoTextLayer, getQgisMapSheetDataset, getQgisProjectMeta } from '../qgis.ts';
import type { ExportOptions } from '../stac.ts';
import type { ExportAsset } from './export.options.ts';
import { cache, tempLocation } from './shared.args.ts';

interface TestProject {
  name: string; // Matches the project filename from the input project
  layout: string;
  sheetCodes: string[];
  dpi: number;
  excludeLayers?: string[];
}

const defaultTests: TestProject[] = [
  {
    name: 'nztopo50',
    layout: 'tiff-50',
    sheetCodes: ['BZ21ptBZ20', 'BQ31', 'BA31', 'BJ29', 'BD36', 'BG39', 'CA11', 'BQ26'],
    dpi: 100,
    excludeLayers: ['Hillshade Igor Color ramp'],
  },
];

export const VisualDiffArgs = {
  worker,
  testFile: option({
    type: optional(Url),
    long: 'test-file',
    description: 'Optional JSON file to override default test projects and their map sheets to export.',
  }),
  project: option({
    type: Url,
    long: 'project',
    description: 'Stac Item path of QGIS Project to use for generate map sheets.',
  }),
  strategy: option({
    type: optional(string),
    long: 'strategy',
    description:
      'Optional storage strategy to filter data collections, e.g. "commit=abc123" or "date=2026-05-19T22-18-14.595Z".',
  }),
  output: option({
    type: Url,
    long: 'output',
    description: 'output local folder to save the exported mapsheets for visual diffing.',
  }),
  tempLocation,
  cache,
};

export const VisualDiffCommand = command({
  name: 'visual-diff',
  description: 'Produce png mapsheets for visual diffing in the pull reqeuest changes',
  args: VisualDiffArgs,
  async handler(args) {
    registerFileSystem();
    const q = qFromArgs(args);
    // Prepare the test scenarios, either from the default tests or from the provided test file
    const testProjects = args.testFile ? await fsa.readJson<TestProject[]>(args.testFile) : defaultTests;

    mkdirSync(args.output, { recursive: true });
    const tasks: Promise<void>[] = [];

    const downloader = new StacDownloader(args.tempLocation, args.cache, q);

    if (args.strategy) {
      downloader.resolvers.unshift(StacDownloader.Resolver.strategy(args.strategy));
      logger.info({ strategy: args.strategy }, 'Visual Diff: Storage strategy override set');
    }

    for (const test of testProjects) {
      if (args.project.href.includes(`${test.name}`)) {
        logger.info({ project: args.project.href }, `Visual Diff: Start`);

        // Download project file, assets, and source data from the project stac file

        const stac = await downloader.fetchStac(args.project);

        logger.info({ resolved: stac.url.href }, 'Visual Diff Project');

        const projectFiles = await downloader.fetchAssets(args.project);
        await downloader.fetchLinkedAssets(args.project, (link) => link.rel === 'dataset');

        // Get the downloaded project file path
        const projectPath = projectFiles.find((f) => f.target.href.includes(`${test.name}.qgs`));
        if (projectPath == null) throw new Error(`Project file not found: ${test.name}.qgs`);

        const projectMeta = await getQgisProjectMeta(projectPath.target);
        const mapSheetLayer = getQgisMapSheetDataset(projectMeta.layers);
        const cartoTextLayer = getQgisCartoTextLayer(projectMeta.layers);

        // Prepare test export options
        const exportAsset: ExportAsset = { layout: test.layout, dpi: test.dpi, format: 'png' };
        const exportOptions: ExportOptions = {
          mapSheetDataset: mapSheetLayer.source,
          cartoTextDataset: cartoTextLayer.source,
          excludeLayers: test.excludeLayers,
          assets: [exportAsset],
        };

        // Start to export file
        const task = test.sheetCodes.map((sheetCode) =>
          q(async () => {
            const file = await pyRunner.qgisExport(
              projectPath.target,
              args.output,
              sheetCode,
              exportOptions,
              exportAsset,
            );
            logger.info({ file: file.href }, `Visual Diff: Exported ${sheetCode}`);
          }),
        );
        tasks.push(...task);
      }
    }
    await Promise.all(tasks);
  },
});
