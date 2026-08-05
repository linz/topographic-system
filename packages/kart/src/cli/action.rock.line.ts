import { getCanonical, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';

import { rockLine } from '../python.runner.ts';
import { DataPrepareArgs, prepareData } from './data.prepare.ts';

const rockLineName = 'nztopo50_rock_line';
const rockLineSchema = new URL('file:///schema/nztopo50_rock_line.json');

export const RockLineArgs = {
  ...DataPrepareArgs,
  marine: option({
    type: Url,
    long: 'marine',
    description: 'Path or s3 of marine stac collection',
  }),
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
  water: option({
    type: Url,
    long: 'water',
    description: 'Path or s3 of water stac collection',
  }),
};

export const RockLineCommand = command({
  name: 'rock line',
  description: 'Rock Line',
  args: RockLineArgs,
  async handler(args) {
    registerFileSystem();
    const marineUrl = await getCanonical(args.marine);
    const coastlineUrl = await getCanonical(args.coastline);
    const islandUrl = await getCanonical(args.island);
    const waterUrl = await getCanonical(args.water);

    await prepareData({
      name: rockLineName,
      label: 'rock line',
      sources: [marineUrl, coastlineUrl, islandUrl, waterUrl],
      run: ([marine, coastline, island, water], output) => rockLine(marine, coastline, island, water, output),
      output: args.output,
      tempLocation: args.tempLocation,
      cache: args.cache,
      concurrency: args.concurrency,
      schema: args.schema ?? rockLineSchema,
    });
  },
});
