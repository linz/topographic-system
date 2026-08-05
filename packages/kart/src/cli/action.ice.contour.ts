import { getCanonical, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';

import { iceContour } from '../python.runner.ts';
import { DataPrepareArgs, prepareData } from './data.prepare.ts';

const iceContourName = 'nztopo50_ice_contour';
const iceContourSchema = new URL('file:///schema/nztopo50_ice_contour.json');

export const IceContourArgs = {
  ...DataPrepareArgs,
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
};

export const IceContourCommand = command({
  name: 'ice contour',
  description: 'Ice Contour',
  args: IceContourArgs,
  async handler(args) {
    registerFileSystem();
    const contourUrl = await getCanonical(args.contour);
    const landcoverUrl = await getCanonical(args.landcover);

    await prepareData({
      name: iceContourName,
      label: 'ice contour',
      sources: [contourUrl, landcoverUrl],
      run: ([contour, landcover], output) => iceContour(contour, landcover, output),
      output: args.output,
      tempLocation: args.tempLocation,
      cache: args.cache,
      concurrency: args.concurrency,
      schema: args.schema ?? iceContourSchema,
    });
  },
});
