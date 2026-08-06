import { getCanonical, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';

import { seaPolygon } from '../python.runner.ts';
import { DataPrepareArgs, prepareData } from './data.prepare.ts';

const seaPolygonName = 'nztopo50_sea_polygon';
const seaPolygonSchema = new URL('file:///schema/nztopo50_sea_polygon.json');

export const SeaPolygonArgs = {
  ...DataPrepareArgs,
  coastline: option({
    type: Url,
    long: 'coastline',
    description: 'Path or s3 of the coastline polygon stac collection to invert into sea polygons',
  }),
};

export const SeaPolygonCommand = command({
  name: 'sea polygon',
  description: 'Build the sea (moana) polygons for the water layer by inverting the derived land polygon layer',
  args: SeaPolygonArgs,
  async handler(args) {
    registerFileSystem();
    const coastlineUrl = await getCanonical(args.coastline);

    await prepareData({
      name: seaPolygonName,
      label: 'sea polygon',
      sources: [coastlineUrl],
      run: ([coastline], output) => seaPolygon(coastline, output),
      output: args.output,
      tempLocation: args.tempLocation,
      cache: args.cache,
      concurrency: args.concurrency,
      schema: args.schema ?? seaPolygonSchema,
    });
  },
});
