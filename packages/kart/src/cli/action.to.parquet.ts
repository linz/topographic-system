import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { upsertAssetToItem, upsertItemToCollection } from '@topographic-system/shared/src/stac.ts';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import os from 'os';
import { basename } from 'path';
import { $ } from 'zx';

import { determineAssetLocation, is_merge_to_master, is_release, recursiveFileSearch } from '../util.ts';

const Concurrency = os.cpus().length;
const Q = new ConcurrentQueue(Concurrency);

export const parquetCommand = command({
  name: 'to-parquet',
  description: 'Convert gpkg files in a folder to parquet format',
  args: {
    compression: option({
      type: optional(string),
      long: 'compression',
      description: 'compression type for parquet files (default: zstd)',
      defaultValue: () => 'zstd',
    }),
    compression_level: option({
      type: optional(number),
      long: 'compression-level',
      description: 'compression level for parquet files (default: 17)',
      defaultValue: () => 17,
    }),
    // Note: inverted logic due to bug/feature in cmd-ts flag defaults (flag not set always means false, regardless of defaultValue)
    no_sort_by_bbox: flag({
      type: boolean,
      defaultValue: () => false,
      long: 'no-sort-by-bbox',
      description: 'whether to _not_ sort parquet files by bounding box (default: false)',
    }),
    row_group_size: option({
      type: optional(number),
      long: 'row-group-size',
      description: 'row group size for parquet files (default: 4096)',
      defaultValue: () => 4096,
    }),
    sourceFiles: restPositionals({
      type: string,
      description: 'List of folders or files to convert (default: all .gpkg files in ./export)',
    }),
  },
  async handler(args) {
    registerFileSystem();

    logger.info(
      {
        concurrency: Concurrency,
        compression: args.compression,
        compression_level: args.compression_level,
        sort_by_bbox: !args.no_sort_by_bbox,
      },
      'ToParquet:Start',
    );

    const sourceFileArguments = args.sourceFiles.length > 0 ? args.sourceFiles : ['./export'];
    const gpkgFilesToProcess = (
      await Promise.all(sourceFileArguments.map((sourceFile) => recursiveFileSearch(fsa.toUrl(sourceFile), '.gpkg')))
    ).flat();

    if (gpkgFilesToProcess.length === 0) {
      logger.info('ToParquet:No files to process');
      return;
    }

    const parquetDir = './parquet';
    await $`mkdir -p ${parquetDir}`;
    logger.info({ gpkgFilesToProcess: gpkgFilesToProcess.map((url) => url.pathname) }, 'ToParquet:Processing');
    for (const gpkgFile of gpkgFilesToProcess) {
      Q.push(async () => {
        const dataset = basename(gpkgFile.href, '.gpkg');
        const parquetFile = `${parquetDir}/${dataset}.parquet`;
        const command = [
          'ogr2ogr',
          '-f',
          'Parquet',
          parquetFile,
          gpkgFile.pathname,
          '-dsco',
          `COMPRESSION=${args.compression}`,
          '-dsco',
          `COMPRESSION_LEVEL=${args.compression_level}`,
          '-dsco',
          `ROW_GROUP_SIZE=${args.row_group_size}`,
        ];
        if (!args.no_sort_by_bbox) {
          command.push('-dsco', 'SORT_BY_BBOX=YES');
        }
        await $`${command}`;
        const assetFile = determineAssetLocation('data', dataset, parquetFile);
        logger.info({ assetFile: assetFile.href }, 'ToParquet:UploadingParquet');
        await fsa.write(assetFile, fsa.readStream(fsa.toUrl(parquetFile)), {
          contentType: 'application/vnd.apache.parquet',
        });
        const stacItemFile = await upsertAssetToItem(assetFile);
        if (is_merge_to_master()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingNextCollection');
          await upsertItemToCollection(stacItemFile, new URL('../../next/collection.json', stacItemFile));
        } else if (is_release()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingLatestCollection');
          await upsertItemToCollection(stacItemFile, new URL('../../latest/collection.json', stacItemFile));
        }
        logger.info({ parquetFile, stacItemFile: stacItemFile.href }, 'ToParquet:Completed');
      });
    }

    await Q.join().catch((err: unknown) => {
      logger.fatal({ err }, 'ToParquet:Error');
      throw err;
    });
    logger.info('ToParquet:Completed');
  },
});
