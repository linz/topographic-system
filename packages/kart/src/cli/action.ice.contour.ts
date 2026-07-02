import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import {
  CliId,
  getCanonical,
  logger,
  registerFileSystem,
  Url,
  UrlFolder,
  stringToUrlFolder,
  qFromArgs,
  concurrency,
  parquetToStac,
  Downloader,
} from '@linzjs/topographic-system-shared';
import { StacCollectionWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, option } from 'cmd-ts';
import type { StacCollection } from 'stac-ts';

import { iceContour } from '../python.runner.ts';

export const IceContourArgs = {
  concurrency,
  contour: option({
    type: Url,
    long: 'contour',
    description: 'Path or s3 of contour stac collection',
  }),
  landcover: option({
    type: Url,
    long: 'landcover',
    description: 'Path or s3 of landcover stac collection',
  }),
  output: option({
    type: UrlFolder,
    long: 'output',
    description: 'Path or s3 of output directory to write to',
  }),
  tempLocation: option({
    type: UrlFolder,
    long: 'temp-location',
    description: 'Where temporary files are stored, generally in /tmp/...',
    defaultValue: () => stringToUrlFolder(path.join(tmpdir(), `topo-system-${CliId}`)),
  }),
  cache: option({
    type: UrlFolder,
    long: 'cache',
    description: 'Optional local cache for storing versioned map assets',
    defaultValue: () => fsa.toUrl('./.cache'),
  }),
};

const iceContourName = 'nztopo50_ice_contour';

export const IceContourCommand = command({
  name: 'ice contour',
  description: 'Ice Contour',
  args: IceContourArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ args }, 'Prepare ice contour: Started');
    const rootCatalog = new URL('catalog.json', args.output);
    const q = qFromArgs(args);

    const contourUrl = await getCanonical(args.contour);
    const landcoverUrl = await getCanonical(args.landcover);

    const latestCollectionUrl = new URL(`${iceContourName}/latest/collection.json`, args.output);
    if (await fsa.exists(latestCollectionUrl)) {
      const latestCollection = await fsa.readJson<StacCollection>(latestCollectionUrl);
      if (
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === contourUrl.href) &&
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === landcoverUrl.href)
      ) {
        logger.info(
          'Latest output collection is already up to date with contour and landcover source, skipping processing',
        );
        logger.info('IceContour: Skip');
        return;
      }
    }

    const downloader = new Downloader(args.tempLocation, args.cache, q);
    downloader.addStac(contourUrl);
    downloader.addStac(landcoverUrl);
    const contourAsset = await downloader.getAsset(contourUrl);
    const landcoverAsset = await downloader.getAsset(landcoverUrl);
    const contourPath = contourAsset[0]?.linked;
    const landcoverPath = landcoverAsset[0]?.linked;
    if (contourPath == null || landcoverPath == null) {
      throw new Error('Failed to download contour or landcover assets');
    }

    const tempOutputParquet = new URL(`${iceContourName}.parquet`, args.tempLocation);

    await iceContour(contourPath, landcoverPath, tempOutputParquet);

    const parquetStats = await parquetToStac(tempOutputParquet);

    const sw = new StacCollectionWriter('data', iceContourName);

    sw.asset('parquet', tempOutputParquet, {
      href: `./${iceContourName}.parquet`,
      type: 'application/vnd.apache.parquet',
      roles: ['data'],
      ...parquetStats.table,
    });

    sw.collection.links.push({ rel: 'derived_from', href: contourUrl.href });
    sw.collection.links.push({ rel: 'derived_from', href: landcoverUrl.href });
    sw.collection.extent = parquetStats.extent;

    const collections = await sw.write(rootCatalog, q);

    await StacUpdater.collections(rootCatalog, [collections], true);
  },
});
