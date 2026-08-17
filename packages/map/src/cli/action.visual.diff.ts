import { mkdirSync } from 'fs';

import { fsa } from '@chunkd/fs';
import {
  Downloader,
  DownloadRels,
  logger,
  qFromArgs,
  registerFileSystem,
  Url,
  worker,
} from '@linzjs/topographic-system-shared';
import { getCollectionsByStrategy, parseStrategy } from '@linzjs/topographic-system-stac';
import { command, option, optional, string } from 'cmd-ts';
import type { StacItem } from 'stac-ts';

import { pyRunner } from '../python.runner.ts';
import { getQgisProjectMeta, getQgisMapSheetDataset, getQgisCartoTextLayer } from '../qgis.ts';
import type { ExportOptions } from '../stac.ts';
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
  catalog: option({
    type: optional(Url),
    long: 'catalog',
    description: 'Optional catalog.json URL to use with --commit-sha for filtering collections by commit.',
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
    // Prepare the test senarios, either from the default tests or from the provided test file
    let testProjects = defaultTests;
    if (args.testFile) {
      testProjects = await fsa.readJson<TestProject[]>(args.testFile);
    }

    mkdirSync(args.output, { recursive: true });
    const tasks = [];

    // Download local data if provided, and add the data path to stac for exporting
    const downloader = new Downloader(args.tempLocation, args.cache, q);

    // Use strategy-based filtering if both strategy and catalog are provided
    if (args.strategy && args.catalog) {
      const [storageStrategy] = parseStrategy(args.strategy);
      if (!storageStrategy) throw new Error(`Invalid strategy: ${args.strategy}`);
      logger.info(
        { strategy: args.strategy, catalog: args.catalog.href },
        'Visual Diff: Filtering collections by strategy',
      );
      const collectionsByCommit = await getCollectionsByStrategy(args.catalog, storageStrategy, q);

      for (const [layerName, collectionUrl] of collectionsByCommit) {
        logger.info({ layer: layerName, collection: collectionUrl.href }, 'Visual Diff: Adding collection');
        downloader.addStac(collectionUrl);
      }

      // Download all assets
      await downloader.getAllAssets({ skipIfExists: false, useCanonical: true });
    }

    for (const test of testProjects) {
      if (args.project.href.includes(`${test.name}`)) {
        logger.info({ project: args.project.href }, `Visual Diff: Start`);

        // Download project file, assets, and source data from the project stac file

        const stac = await fsa.readJson<StacItem>(args.project);
        if (stac == null) throw new Error(`Invalid STAC Item at path: ${args.project.href}`);

        // Add links from download rels for downloading
        downloader.addStac(args.project);
        downloader.addStacLinks(stac, DownloadRels, args.project);

        // Download all the assets, including the project file and source data for the project.
        await downloader.getAllAssets({ skipIfExists: true, useCanonical: true });

        // Get the downloaded project file path
        const projectPath = downloader.findAsset((asset) => asset.url.href.includes(`${test.name}.qgs`))?.linked;
        if (projectPath == null) throw new Error(`Project file not found: ${test.name}.qgs`);

        const projectMeta = await getQgisProjectMeta(projectPath);
        const mapSheetLayer = getQgisMapSheetDataset(projectMeta.layers);
        const cartoTextLayer = getQgisCartoTextLayer(projectMeta.layers);

        // Prepare test export options
        const exportOptions: ExportOptions = {
          layout: test.layout,
          dpi: test.dpi,
          mapSheetDataset: mapSheetLayer.source,
          cartoTextDataset: cartoTextLayer.source,
          format: 'png',
          excludeLayers: test.excludeLayers,
        };

        // Start to export file
        const task = test.sheetCodes.map((sheetCode) =>
          q(async () => {
            const file = await pyRunner.qgisExport(projectPath, args.output, sheetCode, exportOptions);
            logger.info({ file: file.href }, `Visual Diff: Exported ${sheetCode}`);
          }),
        );
        tasks.push(...task);
      }
    }
    await Promise.all(tasks);
  },
});
