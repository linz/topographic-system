import { basename } from 'path';

import { fsa } from '@chunkd/fs';
import {
  ConcurrentQueue,
  determineAssetLocation,
  isMergeToMaster,
  logger,
  recursiveFileSearch,
  registerFileSystem,
  upsertAssetToCollection,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
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
      description: 'compression type for parquet files',
      defaultValue: () => 'zstd',
      defaultValueIsSerializable: true,
    }),
    compressionLevel: option({
      type: optional(number),
      long: 'compression-level',
      description: 'compression level for parquet files',
      defaultValue: () => 17,
      defaultValueIsSerializable: true,
    }),
    sortByBbox: flag({
      type: boolean,
      onMissing: () => true,
      long: 'sort-by-bbox',
      description: 'whether to sort parquet files by bounding box (default: true)',
    }),
    rowGroupSize: option({
      type: optional(number),
      long: 'row-group-size',
      description: 'row group size for parquet files',
      defaultValue: () => 4096,
      defaultValueIsSerializable: true,
    }),
    output: option({
      type: UrlFolder,
      long: 'output',
      description: 'Destination for parquet files and STAC',
    }),
    tempLocation: option({
      type: UrlFolder,
      long: 'temp-location',
      description: 'Temporary location for intermediate files',
      defaultValue: () => new URL('file:///tmp/kart/parquet/'),
      defaultValueIsSerializable: true,
    }),
    sourceFiles: restPositionals({
      type: string,
      description: 'List of folders or files to convert (default: all .gpkg files in ./export)',
    }),
  },
  async handler(args) {
    registerFileSystem();

    const rootCatalog = new URL('catalog.json', args.output);

    logger.info(
      {
        concurrency: Concurrency,
        compression: args.compression,
        compressionLevel: args.compressionLevel,
        sortByBbox: args.sortByBbox,
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

    await $`mkdir -p ${args.tempLocation.pathname}`;
    logger.info({ gpkgFilesToProcess: gpkgFilesToProcess.map((url: URL) => url.pathname) }, 'ToParquet:Processing');
    for (const gpkgFile of gpkgFilesToProcess) {
      Q.push(async () => {
        const dataset = basename(gpkgFile.pathname, extension);
        const parquetFile = new URL(`${dataset}.parquet`, args.tempLocation);
        logger.trace({ parquetFile: parquetFile.pathname, dataset }, 'ToParquet:DestinationFile');
        const command = [
          'ogr2ogr',
          ['-f', 'Parquet'],
          parquetFile.pathname,
          gpkgFile.pathname,
          ['-lco', `COMPRESSION=${args.compression}`],
          ['-lco', `COMPRESSION_LEVEL=${args.compressionLevel}`],
          ['-lco', `ROW_GROUP_SIZE=${args.rowGroupSize}`],
          ['-lco', 'WRITE_COVERING_BBOX=YES'],
          ['-lco', 'COVERING_BBOX_NAME=bbox'],
        ];
        if (args.sortByBbox) {
          command.push(['-lco', 'SORT_BY_BBOX=YES']);
        }
        await $`${command.flat()}`;
        const assetFile = determineAssetLocation({
          category: 'data',
          dataset,
          file: parquetFile,
          root: args.output,
        });
        logger.info({ assetFile: assetFile.href }, 'ToParquet:UploadingParquet');
        await fsa.write(assetFile, fsa.readStream(parquetFile), {
          contentType: 'application/vnd.apache.parquet',
        });
        const stacCollectionFile = await upsertAssetToCollection(rootCatalog, assetFile);
        logger.info(
          { parquetFile, stacCollectionFile: stacCollectionFile.href },
          'ToParquet:AssetToCollectionUpserted',
        );
        if (isMergeToMaster()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingNextCollection');
          await upsertAssetToCollection(
            rootCatalog,
            assetFile,
            new URL('../../latest/collection.json', stacCollectionFile),
          );
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
