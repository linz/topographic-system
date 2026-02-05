import { fsa } from '@chunkd/fs';
import { CliDate, CliInfo } from '@topographic-system/shared/src/cli.info.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { RootCatalogFile, upsertAssetToCollection } from '@topographic-system/shared/src/stac.ts';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import os from 'os';
import { basename } from 'path';
import { $ } from 'zx';

const Concurrency = os.cpus().length;
const Q = new ConcurrentQueue(Concurrency);

function determineAssetLocation(subdir: string, dataset: string, output: string, tag?: string): URL {
  if (!tag) {
    if (is_merge_to_master() || is_release()) {
      tag = `year=${CliDate.slice(0, 4)}/date=${CliDate}`;
    } else if (is_pr()) {
      const ref = $.env['GITHUB_REF_NAME'] || '';
      const prMatch = ref.match(/(\d+)\/merge/);
      if (prMatch) {
        tag = `pull_request/pr-${prMatch[1]}`;
      } else {
        tag = `pull_request/unknown`;
      }
    } else {
      tag = `dev/${CliInfo.hash}`;
    }
  }
  const s3location = new URL(`${subdir}/${dataset}/${tag}/${basename(output)}`, RootCatalogFile);
  logger.info(
    { subdir, tag, master: is_merge_to_master(), release: is_release(), pr: is_pr(), s3location: s3location.href },
    'DetermineAssetLocation:Variables',
  );
  return s3location;
}

function is_pr(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return ref.startsWith('refs/pull/');
}

function is_merge_to_master(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return !is_pr() && ref.endsWith('/master');
}

function is_release(): boolean {
  const workflow = $.env['GITHUB_WORKFLOW_REF'] || '';
  return is_merge_to_master() && workflow.toLowerCase().includes('release');
}

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

    const gpkgFilesToProcess: string[] = [];
    const sourceFileArguments = args.sourceFiles.length > 0 ? args.sourceFiles : ['./export'];

    for (const sourceFileArgument of sourceFileArguments) {
      const sourcePath = fsa.toUrl(sourceFileArgument);
      const stat = await fsa.head(sourcePath);
      if (stat && stat.isDirectory) {
        const filePaths = await fsa.toArray(fsa.list(sourcePath, { recursive: true }));
        for (const filePath of filePaths) {
          if (filePath.href.endsWith('.gpkg')) {
            gpkgFilesToProcess.push(filePath.pathname);
          }
        }
      } else if (stat) {
        if (sourcePath.href.endsWith('.gpkg')) {
          gpkgFilesToProcess.push(sourcePath.pathname);
        }
      }
    }

    if (gpkgFilesToProcess.length === 0) {
      logger.info('ToParquet:No files to process');
      return;
    }

    const parquetDir = './parquet';
    await $`mkdir -p ${parquetDir}`;
    logger.info({ gpkgFilesToProcess }, 'ToParquet:Processing');
    for (const gpkgFile of gpkgFilesToProcess) {
      Q.push(async () => {
        const dataset = basename(gpkgFile, '.gpkg');
        const parquetFile = `${parquetDir}/${dataset}.parquet`;
        const command = [
          'ogr2ogr',
          '-f',
          'Parquet',
          parquetFile,
          gpkgFile,
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
        logger.info({ assetFile }, 'ToParquet:UploadingParquet');
        await fsa.write(assetFile, fsa.readStream(fsa.toUrl(parquetFile)), {
          contentType: 'application/vnd.apache.parquet',
        });
        const stacItemFile = await upsertAssetToCollection(assetFile);
        logger.info({ parquetFile, stacItemFile: stacItemFile.href }, 'ToParquet:Completed');
        if (is_merge_to_master()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingNextCollection');
          await upsertAssetToCollection(assetFile, new URL('../../next/collection.json', stacItemFile));
        } else if (is_release()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingLatestCollection');
          await upsertAssetToCollection(assetFile, new URL('../../latest/collection.json', stacItemFile));
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
