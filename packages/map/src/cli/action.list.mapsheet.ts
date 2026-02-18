import { fsa } from '@chunkd/fs';
import { downloadFile, downloadFromCollection } from '@topographic-system/shared/src/download.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { Url } from '@topographic-system/shared/src/url.ts';
import { command, option, string } from 'cmd-ts';
import type { StacItem } from 'stac-ts';

import { listMapSheets } from '../python.runner.ts';

export const listMapSheetsArgs = {
  project: option({
    type: Url,
    long: 'project',
    description: 'Stac Item path of QGIS Project to use for generate map sheets.',
  }),
  mapSheetLayer: option({
    type: string,
    long: 'map-sheet-layer',
    description: 'Qgis Map Sheet Layer name to use for export',
    defaultValue: () => 'nz_topo50_map_sheet',
    defaultValueIsSerializable: true,
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

    // Download mapshseet layer data from the project stac file
    const stac = await fsa.readJson<StacItem>(args.project);
    if (stac == null) throw new Error(`Invalid STAC Item at path: ${args.project.href}`);
    for (const link of stac.links) {
      if (link.rel === 'dataset' && link.href.includes(args.mapSheetLayer)) {
        await downloadFromCollection(new URL(link.href));
      }
    }
    // Download project file from the project stac file
    let projectPath;
    for (const [key, asset] of Object.entries(stac.assets)) {
      const downloadedPath = await downloadFile(new URL(asset.href));
      if (key === 'project') projectPath = downloadedPath;
    }
    if (projectPath == null) {
      throw new Error(`Project asset not found in STAC Item: ${args.project.href}`);
    }

    // Run python list map sheets script
    const mapSheets = await listMapSheets(projectPath, args.mapSheetLayer);

    // Write outputs files to destination
    await fsa.write(fsa.toUrl(args.output), JSON.stringify(mapSheets, null, 2));
    logger.info({ count: mapSheets.length }, 'ListMapSheets: Completed');
  },
});
