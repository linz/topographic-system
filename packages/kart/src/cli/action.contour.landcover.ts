import { fsa } from '@chunkd/fs';
import {
  CliDate,
  CliId,
  downloadFromCollection,
  logger,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@topographic-system/shared/src/index.ts';
import { upsertAssetToCollection } from '@topographic-system/shared/src/stac.upsert.ts';
import { command, option } from 'cmd-ts';
import path from 'path';

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

// Prepare a temporary folder to store the source data and processed outputs
const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${CliId}/`));

const topo50ContourName = 'nz_topo50_contour';

export const ContourWithLandcoverCommand = command({
  name: 'contour with landcover',
  description: 'Contour with landcover',
  args: ContourWithLandcoverArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ args }, 'Prepare contour with landcover: Started');

    const contourParquet = await downloadFromCollection(args.contour);
    const landcoverParquet = await downloadFromCollection(args.landcover);

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

    const stacCollectionFile = await upsertAssetToCollection(assetFile);
    logger.info({ assetFile, stacCollectionFile: stacCollectionFile.href }, 'AssetToCollectionUpserted');

    logger.debug({ assetFile }, 'UpdatingLatestCollection');
    await upsertAssetToCollection(assetFile, new URL('../../latest/collection.json', stacCollectionFile));
    logger.info('Prepare contour with landcover: Finished');
  },
});
