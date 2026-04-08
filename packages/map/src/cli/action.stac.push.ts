import { logger, registerFileSystem, Url, UrlFolder } from '@linzjs/topographic-system-shared';
import { StacLoader, StacStorageCategoryTypes, StorageStrategyMulti } from '@linzjs/topographic-system-stac';
import { command, flag, multioption, oneOf, option } from 'cmd-ts';

import { qFromArgs } from '../limit.ts';

export const StacPushArgs = {
  source: option({
    type: Url,
    long: 'source',
    description: 'Source data catalog.json',
  }),
  target: option({
    type: UrlFolder,
    long: 'target',
    description: 'Target location to deploy the files. (eg "s3://linz-topographic/") ',
  }),
  category: option({
    type: oneOf(StacStorageCategoryTypes),
    long: 'category',
    description: `Target storage category ${StacStorageCategoryTypes.join(',')}`,
  }),
  strategies: multioption({
    long: 'strategy',
    type: StorageStrategyMulti,
    description: 'Storage strategies to use, for example --strategy=latest',
  }),
  commit: flag({
    long: 'commit',
    description: 'Actually start the stac push',
    defaultValue: () => false,
    defaultValueIsSerializable: true,
  }),
};

export const StacPushCommand = command({
  name: 'stac-push',
  description: 'Push and upsert STAC file and its assets to the target location',
  args: StacPushArgs,
  async handler(args) {
    registerFileSystem();
    if (args.strategies.length === 0) throw new Error('--strategy is missing');
    const q = qFromArgs(args);
    logger.info({ source: args.source, destination: args.target }, 'StacPush: Started');
    const stacLoader = new StacLoader(args.target, args.category);
    for (const st of args.strategies) stacLoader.strategy(st);
    logger.info({ source: args.source, destination: args.target }, 'StacPush: Load');
    await stacLoader.loadCatalog(args.source);
    logger.info({ source: args.source, destination: args.target }, 'StacPush: Push');
    await stacLoader.push(args.target, q, args.commit);
    logger.info('StacPush: Done');
  },
});
