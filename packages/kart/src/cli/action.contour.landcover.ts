import { fsa } from '@chunkd/fs';
import {
  CliDate,
  CliId,
  downloadFile,
  logger,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@topographic-system/shared/src/index.ts';
import { upsertAssetToCollection } from '@topographic-system/shared/src/stac.upsert.ts';
import { command, option } from 'cmd-ts';
import path from 'path';
import { StacCollection } from 'stac-ts';

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
    description: 'Path or s3 of the output directory to write to',
  }),
};

// Prepare a temporary folder to store the source data and processed outputs
const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${CliId}/`));

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
    const contourParquet = await downloadFile(new URL(contourParquetAsset.href));

    const landcoverCollection = await fsa.readJson<StacCollection>(args.landcover);
    const landcoverParquetAsset = landcoverCollection.assets?.['parquet'];
    if (landcoverParquetAsset == null) {
      throw new Error(`Landcover collection must have a parquet asset: ${args.landcover.toString()}`);
    }
    const landcoverParquet = await downloadFile(new URL(landcoverParquetAsset.href));

    const tempOutputParquet = new URL('nz_topo50_contour.parquet', tmpFolder);

    await contourWithLandcover(contourParquet, landcoverParquet, tempOutputParquet);

    const assetFile = new URL(
      `nz_topo50_contour/year=${CliDate.slice(0, 4)}/date=${CliDate}/nz_topo50_contour.parquet`,
      args.output,
    );

    logger.info({ assetFile }, 'UploadingParquet');
    await fsa.write(assetFile, fsa.readStream(tempOutputParquet), {
      contentType: 'application/vnd.apache.parquet',
    });

    const stacItemFile = await upsertAssetToCollection(assetFile);
    logger.info({ assetFile, stacItemFile: stacItemFile.href }, 'AssetToCollectionUpserted');

    logger.debug({ assetFile }, 'UpdatingLatestCollection');
    await upsertAssetToCollection(assetFile, new URL('../../latest/collection.json', stacItemFile));

    logger.info('Prepare contour with landcover: Finished');
  },
});
