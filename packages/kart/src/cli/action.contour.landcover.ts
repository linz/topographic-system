import { fsa } from '@chunkd/fs';
import {
  CliDate,
  downloadFile,
  logger,
  registerFileSystem,
  tmpFolder,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { upsertAssetToCollection } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';
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
};

const topo50ContourName = 'nz_topo50_contour';

export const ContourWithLandcoverCommand = command({
  name: 'contour with landcover',
  description: 'Contour with landcover',
  args: ContourWithLandcoverArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ args }, 'Prepare contour with landcover: Started');

    const contourCollection = await fsa.readJson<StacCollection>(args.contour);
    const contourParquetAsset = contourCollection.assets?.['parquet'];
    if (contourParquetAsset == null) {
      throw new Error(`Contour collection must have a parquet asset: ${args.contour.toString()}`);
    }

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
        return;
      }
    }

    const contourParquet = await downloadFile(new URL(contourParquetAsset.href));
    const landcoverParquet = await downloadFile(new URL(landcoverParquetAsset.href));

    const tempOutputParquet = new URL(`${topo50ContourName}.parquet`, tmpFolder);

    await contourWithLandcover(contourParquet, landcoverParquet, tempOutputParquet);

    const assetFile = new URL(
      `${topo50ContourName}/year=${CliDate.slice(0, 4)}/date=${CliDate}/${topo50ContourName}.parquet`,
      args.output,
    );

    logger.info({ assetFile }, 'UploadingParquet');
    await fsa.write(assetFile, fsa.readStream(tempOutputParquet), {
      contentType: 'application/vnd.apache.parquet',
    });

    const derivedFromContour = {
      rel: 'derived_from',
      href: contourParquetAsset.href,
    };
    const derivedFromLandcover = {
      rel: 'derived_from',
      href: landcoverParquetAsset.href,
    };
    const stacCollectionFile = await upsertAssetToCollection(assetFile, new URL(`./collection.json`, assetFile), [
      derivedFromContour,
      derivedFromLandcover,
    ]);
    logger.info({ assetFile, stacCollectionFile: stacCollectionFile.href }, 'AssetToCollectionUpserted');

    logger.debug({ assetFile }, 'UpdatingLatestCollection');
    await upsertAssetToCollection(assetFile, new URL('../../latest/collection.json', stacCollectionFile), [
      derivedFromContour,
      derivedFromLandcover,
    ]);
    logger.info('Prepare contour with landcover: Finished');
  },
});
