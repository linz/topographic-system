import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import {
  CliId,
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

import { rockLine } from '../python.runner.ts';

export const RockLineArgs = {
  concurrency,
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

const rockLineName = 'nztopo50_rock_line';

/** Get canonical collection if possible */
async function getCanonical(url: URL): Promise<URL> {
  const collection = await fsa.readJson<StacCollection>(url);

  const canonicalLink = collection.links.find((link) => link.rel === 'canonical');
  if (canonicalLink) {
    return new URL(canonicalLink.href, url);
  } else {
    return url;
  }
}

/** Download collection parquet and return local path */
async function downloadParquet(downloader: Downloader, url: URL) {
  downloader.addStac(url);
  const asset = await downloader.getAsset(url);
  const linked = asset[0]?.linked;
  if (linked == null) throw new Error(`Failed to download ${url.href} asset`);
  return linked;
}

export const RockLineCommand = command({
  name: 'rock line',
  description: 'Rock Line',
  args: RockLineArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ args }, 'Prepare rock line: Started');
    const rootCatalog = new URL('catalog.json', args.output);
    const q = qFromArgs(args);

    const marineUrl = await getCanonical(args.marine);
    const coastlineUrl = await getCanonical(args.coastline);
    const islandUrl = await getCanonical(args.island);
    const waterUrl = await getCanonical(args.water);

    const latestCollectionUrl = new URL(`${rockLineName}/latest/collection.json`, args.output);
    if (await fsa.exists(latestCollectionUrl)) {
      const latestCollection = await fsa.readJson<StacCollection>(latestCollectionUrl);
      if (
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === marineUrl.href) &&
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === coastlineUrl.href) &&
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === islandUrl.href) &&
        latestCollection.links.find((link) => link.rel === 'derived_from' && link.href === waterUrl.href)
      ) {
        logger.info('Latest output collections are already up to date with rock line sources, skipping processing');
        logger.info('RockLine: Skip');
        return;
      }
    }

    const downloader = new Downloader(args.tempLocation, args.cache, q);
    const marinePath = await downloadParquet(downloader, marineUrl);
    const coastlinePath = await downloadParquet(downloader, coastlineUrl);
    const islandPath = await downloadParquet(downloader, islandUrl);
    const waterPath = await downloadParquet(downloader, waterUrl);

    const tempOutputParquet = new URL(`${rockLineName}.parquet`, args.tempLocation);

    await rockLine(marinePath, coastlinePath, islandPath, waterPath, tempOutputParquet);

    const parquetStats = await parquetToStac(tempOutputParquet);

    const sw = new StacCollectionWriter('data', rockLineName);

    sw.asset('parquet', tempOutputParquet, {
      href: `./${rockLineName}.parquet`,
      type: 'application/vnd.apache.parquet',
      roles: ['data'],
      ...parquetStats.table,
    });

    sw.collection.links.push({ rel: 'derived_from', href: marineUrl.href });
    sw.collection.links.push({ rel: 'derived_from', href: coastlineUrl.href });
    sw.collection.links.push({ rel: 'derived_from', href: islandUrl.href });
    sw.collection.links.push({ rel: 'derived_from', href: waterUrl.href });
    sw.collection.extent = parquetStats.extent;

    const collections = await sw.write(rootCatalog, q);

    await StacUpdater.collections(rootCatalog, [collections], true);
  },
});
