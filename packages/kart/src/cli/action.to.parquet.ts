import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import os from 'os';
import { basename } from 'path';
import { $ } from 'zx';

const Concurrency = os.cpus().length;
const Q = new ConcurrentQueue(Concurrency);

async function createSTACItem(dataset: string, s3location: URL): Promise<URL> {
  const stacFile = fsa.toUrl(`${s3location.href}.json`);
  await fsa.write(
    stacFile,
    JSON.stringify({
      type: 'Feature',
      stac_version: '1.0.0',
      stac_extensions: [],
      id: dataset,
      properties: {},
    }),
  );
  return stacFile;
}

function determineS3Location(dataset: string, output: string): URL {
  // Placeholder function to determine S3 location based on dataset and output
  logger.info($.env['GITHUB_ACTION_REPOSITORY']);
  logger.info($.env['GITHUB_ACTION_REF']);
  logger.info($.env['GITHUB_WORKFLOW_REF']);
  logger.info($.env);
  let tag = 'unknown';
  const repo = ($.env['GITHUB_REPOSITORY'] || 'unknown').split('/')[1];
  // const ref = $.env['GITHUB_ACTION_REF'] || '';
  if (is_merge_to_master()) {
    // add version number and "next" not "latest" ?
    tag = `v${new Date().toISOString()}`;
  }
  if (is_release()) {
    // add version number and "latest" ?
    tag = `v${new Date().toISOString()}`;
  }
  if (is_pr()) {
    const ref = $.env['GITHUB_REF'] || '';
    const prMatch = ref.match(/refs\/pull\/(\d+)\/merge/);
    if (prMatch) {
      tag = `pr-${prMatch[1]}`;
    } else {
      tag = `pr-unknown`;
    }
  }
  // return new URL(`s3://linz-topography/${repo}/${dataset}/${tag}/${output}`);
  return new URL(`s3://linz-topography-nonprod/topo/🚧/${repo}/${dataset}/${tag}/${output}`);
}

// function to determine if current context is a release
function is_release(): boolean {
  // merge target is master, and the workflow is release
  const workflow = $.env['GITHUB_WORKFLOW_REF'] || '';
  return is_merge_to_master() && workflow.toLowerCase().includes('release');
}

function is_pr(): boolean {
  // merge target is not master and there is a PR number
  const ref = $.env['GITHUB_ACTION_REF'] || '';
  return !is_merge_to_master() && ref.startsWith('refs/pull/');
}

function is_merge_to_master(): boolean {
  const ref = $.env['GITHUB_ACTION_REF'] || '';
  return ref.endsWith('/master');
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
    delete $.env['GITHUB_ACTION_REPOSITORY'];
    delete $.env['GITHUB_ACTION_REF'];
    delete $.env['GITHUB_WORKFLOW_REF'];
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

    const filesToProcess: string[] = [];
    const sourceFileArguments = args.sourceFiles.length > 0 ? args.sourceFiles : ['./export'];

    for (const sourceFileArgument of sourceFileArguments) {
      const sourcePath = fsa.toUrl(sourceFileArgument);
      const stat = await fsa.head(sourcePath);
      if (stat && stat.isDirectory) {
        const filePaths = await fsa.toArray(fsa.list(sourcePath, { recursive: true }));
        for (const filePath of filePaths) {
          if (filePath.href.endsWith('.gpkg')) {
            filesToProcess.push(filePath.pathname);
          }
        }
      } else if (stat) {
        if (sourcePath.href.endsWith('.gpkg')) {
          filesToProcess.push(sourcePath.pathname);
        }
      }
    }

    if (filesToProcess.length === 0) {
      logger.info('ToParquet:No files to process');
      return;
    }

    await $`mkdir ./parquet`;
    logger.info({ filesToProcess }, 'ToParquet:Processing');
    for (const file of filesToProcess) {
      Q.push(async () => {
        const dataset = basename(file, '.gpkg');
        const output = `./parquet/${dataset}.parquet`;
        const command = [
          'ogr2ogr',
          '-f',
          'Parquet',
          output,
          file,
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
        const s3location = determineS3Location(dataset, output);
        const stacItem = await createSTACItem(dataset, s3location);
        logger.info({ s3location }, 'ToParquet:Uploading');
        await fsa.write(s3location, fsa.readStream(fsa.toUrl(output)), {
          contentType: 'application/vnd.apache.parquet',
        });
        logger.info({ stacItem }, 'ToParquet:UploadingSTAC');
        await fsa.write(stacItem, JSON.stringify(stacItem, null, 2), {
          contentType: 'application/json',
        });
        logger.info({ output }, 'ToParquet:Converted');
      });
    }

    await Q.join().catch((err: unknown) => {
      logger.fatal({ err }, 'ToParquet:Error');
      throw err;
    });
    logger.info('ToParquet:Completed');
  },
});
