import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { Url, UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, option, string } from 'cmd-ts';

import { listMapSheets } from '../python.runner.ts';
import { downloadFile, downloadFiles } from './action.download.ts';

export const listMapSheetsArgs = {
  project: option({
    type: Url,
    long: 'project',
    description: 'Path or s3 of QGIS Project to use for list map sheets.',
  }),
  source: option({
    type: UrlFolder,
    long: 'source',
    description: 'Path or s3 of source parquet vector layers to use for generate map sheets.',
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

    // Download source files if not exists
    await downloadFiles(args.source);

    // Download project file if not exists
    const projectFile = await downloadFile(new URL(args.project));

    // Run python list map sheets script
    const mapSheets = await listMapSheets(projectFile);

    // Write outputs files to destination
    await fsa.write(fsa.toUrl(args.output), JSON.stringify(mapSheets, null, 2));
    logger.info({ count: mapSheets.length }, 'ListMapSheets: Completed');
  },
});
