import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import {
    CliId,
    getCanonical,
    logger,
    registerFileSystem,
    Url,
    UrlFolder,
    stringToUrlFolder,
    qFromArgs,
    concurrency,
    parquetToStac,
    Downloader,
} from '@linzjs/topographic-system-shared';
import { StacCollectionWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, option } from 'cmd-ts';
import type { StacCollection } from 'stac-ts';

import { coastlinePolygon } from '../python.runner.ts';

export const CoastlinePolygonArgs = {
    concurrency,
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
    output: option({
        type: UrlFolder,
        long: 'output',
        description: 'Path or s3 of output directory to write to',
    }),
    tempLocation: option({
        type: UrlFolder,
        long: 'temp-location',
        description: 'Where temporary files are stored, generally in /tmp/...',
        defaultValue: () => stringToUrlFolder(path.join(tmpdir(), `topo-system-${CliId}`)),
    }),
    cache: option({
        type: UrlFolder,
        long: 'cache',
        description: 'Optional local cache for storing versioned map assets',
        defaultValue: () => fsa.toUrl('./.cache'),
    }),
};

const coastlinePolygonName = 'nztopo50_coastline_island';

export const CoastlinePolygonCommand = command({
    name: 'coastline polygon',
    description: 'Build the coastlines and islands polygon layer from coastline lines and island polygons',
    args: CoastlinePolygonArgs,
    async handler(args) {
        registerFileSystem();
        logger.info({ args }, 'Prepare coastline polygon: Started');
        const rootCatalog = new URL('catalog.json', args.output);
        const q = qFromArgs(args);
        const downloader = new Downloader(args.tempLocation, args.cache, q);

        const coastlineUrl = await getCanonical(args.coastline);
        const coastlineCollection = await fsa.readJson<StacCollection>(coastlineUrl);

        const coastlineParquetAsset = coastlineCollection.assets?.['parquet'];
        if (coastlineParquetAsset == null) {
            throw new Error(`Coastline collection must have a parquet asset: ${coastlineUrl.toString()}`);
        }

        const islandUrl = await getCanonical(args.island);
        const islandCollection = await fsa.readJson<StacCollection>(islandUrl);
        const islandParquetAsset = islandCollection.assets?.['parquet'];
        if (islandParquetAsset == null) {
            throw new Error(`Island collection must have a parquet asset: ${islandUrl.toString()}`);
        }

        const latestCollectionUrl = new URL(`${coastlinePolygonName}/latest/collection.json`, args.output);
        if (await fsa.exists(latestCollectionUrl)) {
            const latestCollection = await fsa.readJson<StacCollection>(latestCollectionUrl);
            if (
                latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === coastlineParquetAsset.href) &&
                latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === islandParquetAsset.href)
            ) {
                logger.info(
                    'Latest output collection is already up to date with coastline and island source, skipping processing',
                );
                logger.info('CoastlinePolygon: Skip');
                return;
            }
        }

        downloader.addStac(coastlineUrl);
        downloader.addStac(islandUrl);
        const coastlineAsset = await downloader.getAsset(coastlineUrl);
        const islandAsset = await downloader.getAsset(islandUrl);
        const coastlinePath = coastlineAsset[0]?.linked;
        const islandPath = islandAsset[0]?.linked;
        if (coastlinePath == null || islandPath == null) {
            throw new Error('Failed to download coastline or island assets');
        }

        const tempOutputParquet = new URL(`${coastlinePolygonName}.parquet`, args.tempLocation);

        await coastlinePolygon(coastlinePath, islandPath, tempOutputParquet);

        const parquetStats = await parquetToStac(tempOutputParquet);

        const sw = new StacCollectionWriter('data', coastlinePolygonName);

        sw.asset('parquet', tempOutputParquet, {
            href: `./${coastlinePolygonName}.parquet`,
            type: 'application/vnd.apache.parquet',
            roles: ['data'],
            ...parquetStats.table,
        });

        sw.collection.links.push({ rel: 'derived_from', href: coastlineParquetAsset.href });
        sw.collection.links.push({ rel: 'derived_from', href: islandParquetAsset.href });
        sw.collection.extent = parquetStats.extent;

        const collections = await sw.write(rootCatalog, q);

        await StacUpdater.collections(rootCatalog, [collections], true);
    },
});
