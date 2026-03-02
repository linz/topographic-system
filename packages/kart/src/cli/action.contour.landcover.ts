import { logger, Url } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';

import { contourWithLandcover } from '../python.runner.ts';

export const ContourWithLandcoverArgs = {
  contour: option({
    type: Url,
    long: 'contour',
    description: 'Path or s3 of contour parquet',
  }),
  landcover: option({
    type: Url,
    long: 'landcover',
    description: 'Path or s3 of landcover parquet',
  }),
  output: option({
    type: Url,
    long: 'output',
    description: 'Path or s3 of output parquet',
  }),
};

export const ContourWithLandcoverCommand = command({
  name: 'contour with landcover',
  description: 'Contour with landcover',
  args: ContourWithLandcoverArgs,
  async handler(args) {
    logger.info({ args }, 'Prepare contour with landcover: Started');

    await contourWithLandcover(args.contour, args.landcover, args.output);

    logger.info('Prepare contour with landcover: Finished');
  },
});
