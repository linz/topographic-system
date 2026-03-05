import { mkdirSync } from 'fs';

import { fsa } from '@chunkd/fs';
import { downloadProject, logger, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option, optional } from 'cmd-ts';

import { qgisExport } from '../python.runner.ts';
import type { ExportOptions } from '../stac.ts';

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
    sheetCodes: ['BZ21ptBZ20', 'BQ31', 'BA32', 'BJ29', 'BX32', 'BA31'],
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
};

export const VisualDiffCommand = command({
  name: 'visual-diff',
  description: 'Produce png mapsheets for visual diffing in the pull reqeuest changes',
  args: VisualDiffArgs,
  async handler(args) {
    registerFileSystem();
    // Prepare the test senarios, either from the default tests or from the provided test file
    let testProjects = defaultTests;
    if (args.testFile) {
      testProjects = await fsa.readJson<TestProject[]>(args.testFile);
    }

    mkdirSync(args.output, { recursive: true });

    for (const test of testProjects) {
      if (args.project.href.includes(`${test.name}`)) {
        logger.info({ project: args.project.href }, `Visual Diff: Start`);

        // Download project file, assets, and source data from the project stac file
        const projectPath = await downloadProject(args.project);

        // Prepare test export options
        const exportOptions: ExportOptions = {
          mapSheetLayer: test.mapSheetLayer,
          layout: test.layout,
          dpi: test.dpi,
          format: 'png',
        };

        // Start to export file
        for (const sheetCode of test.sheetCodes) {
          const file = await qgisExport(projectPath, args.output, sheetCode, exportOptions);
          logger.info({ file: file.href }, `Visual Diff: Exported ${sheetCode}`);
        }
      }
    }
  },
});
