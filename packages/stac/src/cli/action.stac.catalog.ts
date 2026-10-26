import { fsa } from '@chunkd/fs';
import { logger, registerFileSystem, UrlFolder } from '@linzjs/topographic-system-shared';
import { command, flag, option } from 'cmd-ts';

import { StacUpdater } from '../stac.update.ts';

export const StacCatalogArgs = {
    target: option({
        type: UrlFolder,
        long: 'target',
        description:
            'Folder containing dataset sub-folders, each with a collection.json. A root catalog.json linking every collection is written here.',
    }),
    commit: flag({
        long: 'commit',
        description: 'Actually write the catalog.json',
        defaultValue: () => false,
        defaultValueIsSerializable: true,
    }),
};

export const StacCatalogCommand = command({
    name: 'stac-catalog',
    description: 'Build a root catalog.json that links every collection.json found under the target folder',
    args: StacCatalogArgs,
    async handler(args) {
        registerFileSystem();
        const rootCatalogUrl = new URL('catalog.json', args.target);

        const files = await fsa.toArray(fsa.list(args.target, { recursive: true }));
        const collections = files.filter((f) => f.href.endsWith('/collection.json'));
        if (collections.length === 0) throw new Error(`No collection.json found under ${args.target.href}`);
        logger.info({ target: args.target.href, collections: collections.length }, 'StacCatalog: Found collections');

        const catalogs = await StacUpdater.collections(rootCatalogUrl, collections, args.commit);
        for (const catalog of catalogs) logger.info({ href: catalog.href }, 'StacCatalog: Catalog upserted');
        logger.info({ catalogs: catalogs.length, dryRun: args.commit ? false : true }, 'StacCatalog: Done');
    },
});
