import {
  concurrency,
  logger,
  qFromArgs,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';

import { StacDownloader } from '@linzjs/topographic-system-stac';
import { DownloadRels } from './action.export.ts';
import { cache } from './shared.args.ts';

export const DownloadArgs = {
  concurrency,
  project: option({
    type: Url,
    long: 'project',
    description: 'Stac Item path of QGIS Project to download.',
  }),
  output: option({
    type: UrlFolder,
    long: 'output',
    description: 'Path or s3 bucket of the output directory to write downloaded items.',
  }),
  cache,
};

export const DownloadCommand = command({
  name: 'download',
  description: 'Download a QGIS project and all related parquet and source dataset files into output directory.',
  args: DownloadArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project.href, output: args.output.href, cache: args.cache.href }, 'Download: Start');

    const q = qFromArgs(args);

    const downloader = new StacDownloader(args.output, args.cache, q);
    await downloader.fetchLinkedAssets(args.project, (link) => DownloadRels.has(link.rel));

    logger.info({ project: args.project.href, output: args.output.href }, 'Download: End');
  },
});
