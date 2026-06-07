import { mkdirSync } from 'fs';

import { fsa } from '@chunkd/fs';
import {
  Downloader,
  DownloadRels,
  logger,
  qFromArgs,
  registerFileSystem,
  Url,
  UrlFolder,
  worker,
} from '@linzjs/topographic-system-shared';
import { command, option, optional } from 'cmd-ts';
import type { StacItem } from 'stac-ts';

import { pyRunner } from '../python.runner.ts';
import type { ExportOptions } from '../stac.ts';
import { cache, tempLocation } from './shared.args.ts';

interface TestProject {
  name: string; // Matches the project filename from the input project
  mapSheetLayer: string;
  layout: string;
  sheetCodes: string[];
  dpi: number;
  excludeLayers?: string[];
}

const defaultTests: TestProject[] = [
  {
    name: 'nztopo50',
    mapSheetLayer: 'nz_topo50_map_sheet',
    layout: 'tiff-50',
    sheetCodes: ['BZ21ptBZ20', 'BQ31', 'BA31', 'BJ29', 'BX32', 'BD36', 'BG39', 'CA11', 'BQ26'],
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
  data: option({
    type: optional(UrlFolder),
    long: 'data',
    description:
      'Optional local path to download the source data for the project, that override the default data from project.',
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
    if (args.data) {
      const files = await fsa.toArray(fsa.list(args.data));
      for (const file of files) {
        if (!file.href.endsWith('.json')) continue;
        if (file.href.endsWith('catalog.json')) continue;
        downloader.addStac(file);
      }
      await downloader.getAllAssets();
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
        await downloader.getAllAssets(true);

        // Prepare test export options
        const exportOptions: ExportOptions = {
          mapSheetLayer: test.mapSheetLayer,
          layout: test.layout,
          dpi: test.dpi,
          format: 'png',
          excludeLayers: test.excludeLayers,
        };

        // Get the downloaded project file path
        const projectPath = downloader.findAsset((asset) => asset.url.href.includes(`${test.name}.qgs`))?.linked;
        if (projectPath == null) throw new Error(`Project file not found: ${test.name}.qgs`);

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
