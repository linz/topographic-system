import { mkdirSync } from 'fs';

import { fsa } from '@chunkd/fs';
import { downloadProject, logger, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, number, option, optional } from 'cmd-ts';
import pLimit from 'p-limit';

import { pyRunner } from '../python.runner.ts';
import type { ExportOptions } from '../stac.ts';
import { tempLocation } from './shared.args.ts';

interface TestProject {
  name: string; // Matches the project filename from the input project
  mapSheetLayer: string;
  layout: string;
  sheetCodes: string[];
  dpi: number;
}

const defaultTests: TestProject[] = [
  {
    name: 'nz-topo50-map',
    mapSheetLayer: 'nz_topo50_map_sheet',
    layout: 'tiff-50',
    sheetCodes: ['BZ21ptBZ20', 'BQ31', 'BA31', 'BJ29', 'BX32', 'BD36', 'BG39', 'CA11', 'BQ26'],
    dpi: 200,
  },
];

export const VisualDiffArgs = {
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
  output: option({
    type: Url,
    long: 'output',
    description: 'output local folder to save the exported mapsheets for visual diffing.',
  }),
  concurrency: option({
    type: number,
    long: 'concurrency',
    description: 'Number of concurrent exports to run (default: 20)',
    defaultValue: () => 20,
    defaultValueIsSerializable: true,
  }),
  tempLocation,
};

export const VisualDiffCommand = command({
  name: 'visual-diff',
  description: 'Produce png mapsheets for visual diffing in the pull reqeuest changes',
  args: VisualDiffArgs,
  async handler(args) {
    registerFileSystem();
    const q = pLimit(args.concurrency);
    // Prepare the test senarios, either from the default tests or from the provided test file
    let testProjects = defaultTests;
    if (args.testFile) {
      testProjects = await fsa.readJson<TestProject[]>(args.testFile);
    }

    mkdirSync(args.output, { recursive: true });
    const tasks = [];

    for (const test of testProjects) {
      if (args.project.href.includes(`${test.name}`)) {
        logger.info({ project: args.project.href }, `Visual Diff: Start`);

        // Download project file, assets, and source data from the project stac file
        const projectPath = await downloadProject(args.project, args.tempLocation);

        // Prepare test export options
        const exportOptions: ExportOptions = {
          mapSheetLayer: test.mapSheetLayer,
          layout: test.layout,
          dpi: test.dpi,
          format: 'png',
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
