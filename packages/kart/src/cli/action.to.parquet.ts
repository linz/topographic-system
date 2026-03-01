import { fsa } from '@chunkd/fs';
import { CliDate, CliInfo } from '@topographic-system/shared/src/cli.info.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { RootCatalogFile } from '@topographic-system/shared/src/stac.constants.ts';
import { upsertAssetToCollection } from '@topographic-system/shared/src/stac.upsert.ts';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import { basename } from 'path';
import { $ } from 'zx';

const Concurrency = 1; // os.cpus().length - Fixme: race conditions when writing STAC files; setting this to 1 for now to ensure correctness, but ideally should be able to run in parallel
const Q = new ConcurrentQueue(Concurrency);

function determineAssetLocation(subdir: string, dataset: string, output: string, tag?: string): URL {
  if (tag == null) {
    if (isMergeToMaster() || isRelease()) {
      tag = `year=${CliDate.slice(0, 4)}/date=${CliDate}`;
    } else if (isPullRequest()) {
      const ref = $.env['GITHUB_REF'] ?? '';
      const prMatch = ref.match(/refs\/pull\/(\d+)/);
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
    { subdir, tag, master: isMergeToMaster(), release: isRelease(), pr: isPullRequest(), s3location: s3location.href },
    'DetermineAssetLocation:Variables',
  );
  return s3location;
}

function isPullRequest(): boolean {
  const ref = $.env['GITHUB_REF'] ?? '';
  logger.debug({ ref }, 'IsPullRequest:GITHUB_REF');
  return ref.startsWith('refs/pull/');
}

function isMergeToMaster(): boolean {
  const ref = $.env['GITHUB_REF'] ?? '';
  return !isPullRequest() && ref.endsWith('/master');
}

function isRelease(): boolean {
  const workflow = $.env['GITHUB_WORKFLOW_REF'] ?? '';
  logger.debug({ workflow }, 'IsRelease:GITHUB_WORKFLOW_REF');
  return isMergeToMaster() && workflow.toLowerCase().includes('release');
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
          ['-f', 'Parquet'],
          parquetFile,
          gpkgFile,
          ['-lco', `COMPRESSION=${args.compression}`],
          ['-lco', `COMPRESSION_LEVEL=${args.compression_level}`],
          ['-lco', `ROW_GROUP_SIZE=${args.row_group_size}`],
          ['-lco', 'WRITE_COVERING_BBOX=YES'],
          ['-lco', 'COVERING_BBOX_NAME=bbox'],
        ];
        if (!args.no_sort_by_bbox) {
          command.push(['-lco', 'SORT_BY_BBOX=YES']);
        }
        await $`${command.flat()}`;
        const assetFile = determineAssetLocation('data', dataset, parquetFile);
        logger.info({ assetFile }, 'ToParquet:UploadingParquet');
        await fsa.write(assetFile, fsa.readStream(fsa.toUrl(parquetFile)), {
          contentType: 'application/vnd.apache.parquet',
        });
        const stacItemFile = await upsertAssetToCollection(assetFile);
        logger.info({ parquetFile, stacItemFile: stacItemFile.href }, 'ToParquet:AssetToCollectionUpserted');
        if (isRelease()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingLatestCollection');
          await upsertAssetToCollection(assetFile, new URL('../../latest/collection.json', stacItemFile));
        } else if (isMergeToMaster()) {
          logger.debug({ assetFile }, 'ToParquet:UpdatingNextCollection');
          await upsertAssetToCollection(assetFile, new URL('../../next/collection.json', stacItemFile));
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
