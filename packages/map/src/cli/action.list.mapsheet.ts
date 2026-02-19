import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { Url } from '@topographic-system/shared/src/url.ts';
import { command, option, string } from 'cmd-ts';

import { downloadProject } from '../download.ts';
import { listMapSheets } from '../python.runner.ts';

export const listMapSheetsArgs = {
  project: option({
    type: Url,
    long: 'project',
    description: 'Stac Item path of QGIS Project to use for generate map sheets.',
  }),
  output: option({
    type: string,
    long: 'output',
    description: 'Output directory to write the mapsheet json file.',
    defaultValueIsSerializable: true,
    defaultValue: () => '/tmp/mapsheets.json',
  }),
};

export const listMapSheetsCommand = command({
  name: 'list-mapsheets',
  description: 'Read a Qgis project and list all the mapsheets defined in it.',
  args: listMapSheetsArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project }, 'ListMapSheets: Started');

    // Download project file, assets, and source data from the project stac file
    const { projectPath, sources } = await downloadProject(args.project);
    logger.info({ projectPath: projectPath.href, sourceCount: sources.length }, 'ListMapSheets: Project Downloaded');

    // Run python list map sheets script
    const mapSheets = await listMapSheets(projectPath);

    // Write outputs files to destination
    await fsa.write(fsa.toUrl(args.output), JSON.stringify(mapSheets, null, 2));
    logger.info({ count: mapSheets.length }, 'ListMapSheets: Completed');
  },
});
