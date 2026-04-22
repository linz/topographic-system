import { concurrency, logger, qFromArgs, registerFileSystem, Url, UrlFolder } from '@linzjs/topographic-system-shared';
import {
  StacPusher,
  StacStorageCategoryTypes,
  StacUpdater,
  StorageStrategyMulti,
} from '@linzjs/topographic-system-stac';
import { command, flag, multioption, oneOf, option } from 'cmd-ts';

export const StacPushArgs = {
  concurrency,
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
    const rootCatalogUrl = new URL('catalog.json', args.target);
    const q = qFromArgs(args);
    logger.info({ source: args.source, destination: args.target }, 'StacPush: Started');
    const stacPusher = new StacPusher(args.target, args.category);

    // Set Strategies for StacPusher
    for (const st of args.strategies) stacPusher.strategy(st);

    // Push Stac Item, Collection and Assets
    logger.info({ source: args.source, destination: args.target }, 'StacPush: Push');
    const { items, collections } = await stacPusher.push(args.source, q, args.commit);
    for (const item of items) logger.info({ href: item.href }, 'StacPush: Item pushed');
    for (const collection of collections) logger.info({ href: collection.href }, 'StacPush: Collection pushed');

    // Upsert Stac Catalog
    const catalogs = await StacUpdater.collections(rootCatalogUrl, collections, args.commit);
    for (const catalog of catalogs) logger.info({ href: catalog.href }, 'StacPush: Catalog upserted');

    logger.info(
      {
        items: items.length,
        collections: collections.length,
        catalogs: catalogs.length,
        dryRun: args.commit ? false : true,
      },
      'StacPush: Done',
    );
  },
});
