import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import {
  CliId,
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

    // TODO use canonical
    const contourCollection = await fsa.readJson<StacCollection>(args.contour);
    const contourParquetAsset = contourCollection.assets?.['parquet'];
    if (contourParquetAsset == null) {
      throw new Error(`Contour collection must have a parquet asset: ${args.contour.toString()}`);
    }

    // TODO use canonical
    const landcoverCollection = await fsa.readJson<StacCollection>(args.landcover);
    const landcoverParquetAsset = landcoverCollection.assets?.['parquet'];
    if (landcoverParquetAsset == null) {
      throw new Error(`Landcover collection must have a parquet asset: ${args.landcover.toString()}`);
    }

    const latestCollectionUrl = new URL(`${iceContourName}/latest/collection.json`, args.output);
    if (await fsa.exists(latestCollectionUrl)) {
      const latestCollection = await fsa.readJson<StacCollection>(latestCollectionUrl);
      if (
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === contourParquetAsset.href) &&
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === landcoverParquetAsset.href)
      ) {
        logger.info(
          'Latest output collection is already up to date with contour and landcover source, skipping processing',
        );
        logger.info('IceContour: Skip');
        return;
      }
    }

    const downloader = new Downloader(args.tempLocation, q);
    downloader.addAsset(new URL(contourParquetAsset.href, args.contour));
    downloader.addAsset(new URL(landcoverParquetAsset.href, args.landcover));

    const contourParquet = await downloader.getAsset(new URL(contourParquetAsset.href, args.contour));
    const landcoverParquet = await downloader.getAsset(new URL(landcoverParquetAsset.href, args.landcover));

    const tempOutputParquet = new URL(`${iceContourName}.parquet`, args.tempLocation);

    await iceContour(contourParquet, landcoverParquet, tempOutputParquet);

    const parquetStats = await parquetToStac(tempOutputParquet);

    const sw = new StacCollectionWriter('data', iceContourName);

    sw.asset('parquet', tempOutputParquet, {
      href: `./${iceContourName}.parquet`,
      type: 'application/vnd.apache.parquet',
      roles: ['data'],
      ...parquetStats.table,
    });

    sw.collection.links.push({ rel: 'derived_from', href: contourParquetAsset.href });
    sw.collection.links.push({ rel: 'derived_from', href: landcoverParquetAsset.href });
    sw.collection.extent = parquetStats.extent;

    const collections = await sw.write(rootCatalog, q);

    await StacUpdater.collections(rootCatalog, [collections], true);
  },
});
