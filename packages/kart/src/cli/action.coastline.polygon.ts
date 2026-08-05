import { getCanonical, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';

import { coastlinePolygon } from '../python.runner.ts';
import { DataPrepareArgs, prepareData } from './data.prepare.ts';

const coastlinePolygonName = 'nztopo50_coastline_island';
const coastlinePolygonSchema = new URL('file:///schema/coastline_polygon.json');

export const CoastlinePolygonArgs = {
  ...DataPrepareArgs,
  coastline: option({
    type: Url,
    long: 'coastline',
    description: 'Path or s3 of coastline stac collection',
  }),
  island: option({
    type: Url,
    long: 'island',
    description: 'Path or s3 of island stac collection',
  }),
};

export const CoastlinePolygonCommand = command({
  name: 'coastline polygon',
  description: 'Build the coastlines and islands polygon layer from coastline lines and island polygons',
  args: CoastlinePolygonArgs,
  async handler(args) {
    registerFileSystem();
    const coastlineUrl = await getCanonical(args.coastline);
    const islandUrl = await getCanonical(args.island);

    await prepareData({
      name: coastlinePolygonName,
      label: 'coastline polygon',
      sources: [coastlineUrl, islandUrl],
      run: ([coastline, island], output) => coastlinePolygon(coastline, island, output),
      output: args.output,
      tempLocation: args.tempLocation,
      cache: args.cache,
      concurrency: args.concurrency,
      schema: args.schema ?? coastlinePolygonSchema,
    });
  },
});
