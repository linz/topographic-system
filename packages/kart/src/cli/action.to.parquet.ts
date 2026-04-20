import { mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import {
  concurrency,
  logger,
  qFromArgs,
  recursiveFileSearch,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import type { ParquetStacMetadata } from '@linzjs/topographic-system-shared/src/parquet.metadata.ts';
import { parquetToStac } from '@linzjs/topographic-system-shared/src/parquet.metadata.ts';
import { stringToUrlFolder } from '@linzjs/topographic-system-shared/src/url.ts';
import { StacCollectionWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import { $ } from 'zx';

export const ParquetCommand = command({
  name: 'to-parquet',
  description: 'Convert gpkg files in a folder to parquet format',
  args: {
    concurrency,
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
      description: 'Optional output directory for temporary intermediate results (default: $TMPDIR/kart/parquet)',
      defaultValue: () => stringToUrlFolder(path.join(tmpdir(), 'kart', 'parquet')),
    }),
    sourceFiles: restPositionals({
      type: Url,
      description: 'List of folders or files to convert (default: all .gpkg files in ./export)',
    }),
  },
  async handler(args) {
    registerFileSystem();

    const rootCatalog = new URL('catalog.json', args.output);
    const q = qFromArgs(args);

    logger.info(
      {
        concurrency: args.concurrency,
        compression: args.compression,
        compressionLevel: args.compressionLevel,
        sortByBbox: args.sortByBbox,
      },
      'ToParquet:Start',
    );
    const extension = '.gpkg';
    const sourceFileArguments =
      args.sourceFiles.length > 0 ? args.sourceFiles : [stringToUrlFolder(path.join(tmpdir(), 'kart', 'export'))];
    const gpkgFilesToProcess = (
      await Promise.all(sourceFileArguments.map((sourceFile) => recursiveFileSearch(sourceFile, extension)))
    ).flat();

    if (gpkgFilesToProcess.length === 0) {
      logger.info({ sourceFileArguments }, 'ToParquet:NoFilesToProcess');
      return;
    }

    await mkdir(args.tempLocation, { recursive: true });
    logger.info({ gpkgFilesToProcess: gpkgFilesToProcess.map((url: URL) => url.pathname) }, 'ToParquet:Processing');

    const datasets: { dataset: string; source: URL; metadata: ParquetStacMetadata }[] = [];
    const todo: Promise<unknown>[] = [];
    for (const gpkgFile of gpkgFilesToProcess) {
      todo.push(
        q(async () => {
          const dataset = path.basename(gpkgFile.pathname, extension);
          const parquetFile = new URL(`${dataset}.parquet`, args.tempLocation);
          logger.info({ parquetFile: parquetFile.pathname, dataset }, 'ToParquet:DestinationFile');
          const command = [
            'ogr2ogr',
            ['-f', 'Parquet'],
            fileURLToPath(parquetFile),
            fileURLToPath(gpkgFile),
            ['-lco', `COMPRESSION=${args.compression}`],
            ['-lco', `COMPRESSION_LEVEL=${args.compressionLevel}`],
            ['-lco', `ROW_GROUP_SIZE=${args.rowGroupSize}`],
            ['-lco', 'WRITE_COVERING_BBOX=YES'],
            ['-lco', 'COVERING_BBOX_NAME=bbox'],
            ['-a_srs', 'epsg:4326'],
          ];
          if (args.sortByBbox) command.push(['-lco', 'SORT_BY_BBOX=YES']);
          await $`${command.flat()}`;

          const stat = await fsa.head(parquetFile);

          const parquetStats = await parquetToStac(parquetFile);
          logger.info(
            {
              dataset,
              size: stat?.size,
              fields: parquetStats.table['table:columns'].map((c) => c.name),
              rowCount: parquetStats.table['table:row_count'],
            },
            'ToParquet:Written',
          );
          datasets.push({ dataset, source: parquetFile, metadata: parquetStats });
        }),
      );
    }
    await Promise.all(todo);

    const collections: URL[] = [];
    for (const ds of datasets) {
      const sw = new StacCollectionWriter('data', ds.dataset);
      sw.asset('parquet', ds.source, {
        href: `./${ds.dataset}.parquet`,
        roles: ['data'],
        type: 'application/vnd.apache.parquet',
        ...ds.metadata.table,
      });
      sw.collection.title = ds.dataset; // TODO this should come from `kart meta get`
      sw.collection.description = `topographic-system export of ${ds.dataset}`; // TODO this should come from `kart meta get`
      sw.collection.extent = ds.metadata.extent;
      collections.push(await sw.write(args.output, q));
    }

    await StacUpdater.collections(rootCatalog, collections.flat(), true);

    logger.info('ToParquet:Completed');
  },
});
