import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { recursiveFileSearch } from '@topographic-system/shared/src/fs.util.ts';
import { isMergeToMaster, isRelease } from '@topographic-system/shared/src/github.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { determineAssetLocation } from '@topographic-system/shared/src/stac.links.ts';
import { upsertAssetToCollection } from '@topographic-system/shared/src/stac.upsert.ts';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import { basename } from 'path';
import { $ } from 'zx';

const Concurrency = 1; // os.cpus().length - Fixme: race conditions when writing STAC files; setting this to 1 for now to ensure correctness, but ideally should be able to run in parallel
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
    sort_by_bbox: flag({
      type: boolean,
      onMissing: () => true,
      long: 'sort-by-bbox',
      description: 'whether to sort parquet files by bounding box (default: true)',
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
        sort_by_bbox: args.sort_by_bbox,
      },
      'ToParquet:Start',
    );
    const extension = '.gpkg';
    const sourceFileArguments = args.sourceFiles.length > 0 ? args.sourceFiles : ['./export'];
    const gpkgFilesToProcess = (
      await Promise.all(sourceFileArguments.map((sourceFile) => recursiveFileSearch(fsa.toUrl(sourceFile), extension)))
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
        const dataset = basename(gpkgFile.pathname, extension);
        const parquetFile = `${parquetDir}/${dataset}.parquet`;
        const command = [
          'ogr2ogr',
          ['-f', 'Parquet'],
          parquetFile,
          gpkgFile.pathname,
          ['-lco', `COMPRESSION=${args.compression}`],
          ['-lco', `COMPRESSION_LEVEL=${args.compression_level}`],
          ['-lco', `ROW_GROUP_SIZE=${args.row_group_size}`],
        ];
        if (args.sort_by_bbox) {
          command.push(['-lco', 'SORT_BY_BBOX=YES']);
        }
        await $`${command.flat()}`;
        const assetFile = determineAssetLocation('data', dataset, parquetFile);
        logger.info({ assetFile: assetFile.href }, 'ToParquet:UploadingParquet');
        await fsa.write(assetFile, fsa.readStream(fsa.toUrl(parquetFile)), {
          contentType: 'application/vnd.apache.parquet',
        });
        const stacCollectionFile = await upsertAssetToCollection(assetFile);
        logger.info(
          { parquetFile, stacCollectionFile: stacCollectionFile.href },
          'ToParquet:AssetToCollectionUpserted',
        );
        if (isMergeToMaster()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingNextCollection');
          await upsertAssetToCollection(assetFile, new URL('../../next/collection.json', stacCollectionFile));
        } else if (isRelease()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingLatestCollection');
          await upsertAssetToCollection(assetFile, new URL('../../latest/collection.json', stacCollectionFile));
        }
      });
    }

    await Q.join().catch((err: unknown) => {
      logger.fatal({ err }, 'ToParquet:Error');
      throw err;
    });
    logger.info('ToParquet:Completed');
  },
});
