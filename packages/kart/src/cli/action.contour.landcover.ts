import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import {
  CliId,
  downloadFile,
  logger,
  registerFileSystem,
  Url,
  UrlFolder,
  stringToUrlFolder,
} from '@linzjs/topographic-system-shared';
import { StacCollectionWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, option } from 'cmd-ts';
import pLimit from 'p-limit';
import type { StacCollection } from 'stac-ts';

import { contourWithLandcover } from '../python.runner.ts';

export const ContourWithLandcoverArgs = {
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

const topo50ContourName = 'nz_topo50_contour';

export const ContourWithLandcoverCommand = command({
  name: 'contour with landcover',
  description: 'Contour with landcover',
  args: ContourWithLandcoverArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ args }, 'Prepare contour with landcover: Started');
    const rootCatalog = new URL('catalog.json', args.output);

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

    const latestCollectionUrl = new URL(`${topo50ContourName}/latest/collection.json`, args.output);
    if (await fsa.exists(latestCollectionUrl)) {
      const latestCollection = await fsa.readJson<StacCollection>(latestCollectionUrl);
      if (
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === contourParquetAsset.href) &&
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === landcoverParquetAsset.href)
      ) {
        logger.info(
          'Latest output collection is already up to date with contour and landcover source, skipping processing',
        );
        logger.info('ContourLandcover: Skip');
        return;
      }
    }

    const contourParquet = await downloadFile(new URL(contourParquetAsset.href), args.tempLocation);
    const landcoverParquet = await downloadFile(new URL(landcoverParquetAsset.href), args.tempLocation);

    const tempOutputParquet = new URL(`${topo50ContourName}.parquet`, args.tempLocation);

    await contourWithLandcover(contourParquet, landcoverParquet, tempOutputParquet);

    const sw = new StacCollectionWriter('data', topo50ContourName);

    sw.asset('parquet', tempOutputParquet, {
      href: `./${topo50ContourName}.parquet`,
      type: 'application/vnd.apache.parquet',
      roles: ['data'],
    });

    sw.collection.links.push({ rel: 'derived_from', href: contourParquetAsset.href });
    sw.collection.links.push({ rel: 'derived_from', href: landcoverParquetAsset.href });

    const collections = await sw.write(rootCatalog, pLimit(4), true);

    await StacUpdater.collections(rootCatalog, collections, true);
  },
});
