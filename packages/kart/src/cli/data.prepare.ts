import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import {
  CliId,
  concurrency,
  logger,
  parquetToStac,
  qFromArgs,
  stringToUrlFolder,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { StacCollectionWriter, StacUpdater, StacDownloader } from '@linzjs/topographic-system-stac';
import { option, optional } from 'cmd-ts';
import type { StacCollection } from 'stac-ts';

import { ValidateSchemaCommand } from './action.validate.schema.ts';

export const DataPrepareArgs = {
  concurrency,
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
  schema: option({
    type: optional(Url),
    long: 'schema',
    description: 'Path to the JSON schema to validate the prepared output against',
  }),
};

export interface PrepareDataOptions<T extends readonly URL[]> {
  /** Output collection name, e.g. `nztopo50_rock_line`. */
  name: string;
  /** Human friendly label used in log messages, e.g. `rock line`. */
  label: string;
  /**
   * Canonical source collection URLs, in the order the python runner expects them.
   * Used both to download the source parquet assets and as `derived_from` links.
   */
  sources: readonly [...T];
  /**
   * Run the python cli given the downloaded source parquet paths (same order as
   * {@link PrepareDataOptions.sources}) and the output parquet url.
   */
  run: (sourcePaths: T, output: URL) => Promise<void>;
  /** Output catalog directory. */
  output: URL;
  /** Temporary working directory. */
  tempLocation: URL;
  /** Local cache directory for versioned source assets. */
  cache: URL;
  /** Concurrency limit for parallel processing. */
  concurrency?: number;
  /** JSON schema to validate the output parquet against. */
  schema: URL;
}

/**
 * Shared data-preparation flow used by the derived layer commands (rock line,
 * ice contour, coastline polygon, sea polygon).
 *
 * Steps:
 * 1. Skip when the latest output is already derived from the same sources.
 * 2. Otherwise download the source parquets, run the python cli to prepare the data
 * 3. Validate the output against schema, then write and publish the STAC collection.
 */
export async function prepareData<const T extends readonly URL[]>(opts: PrepareDataOptions<T>): Promise<void> {
  const { name, label, sources, run, output, tempLocation, cache, schema } = opts;

  logger.info({ name }, `Prepare ${label}: Started`);

  const rootCatalog = new URL('catalog.json', output);
  const q = qFromArgs({ concurrency: opts.concurrency });

  const latestCollectionUrl = new URL(`${name}/latest/collection.json`, output);
  if (await fsa.exists(latestCollectionUrl)) {
    const latestCollection = await fsa.readJson<StacCollection>(latestCollectionUrl);
    const derivedUnchanged = sources.every((source) =>
      latestCollection.links.some((link) => link.rel === 'derived_from' && link.href === source.href),
    );
    if (derivedUnchanged) {
      logger.info({ name }, `Prepare ${label}: Skip, latest output is already up to date with sources`);
      return;
    }
  }

  const downloader = new StacDownloader(tempLocation, cache, q);
  const sourceAssets = (await Promise.all(sources.map((m) => downloader.fetchAssets(m)))).flat();
  const sourcePaths = sourceAssets.filter((s) => s.target.pathname.endsWith('.parquet')).map((s) => s.target);

  const tempOutputParquet = new URL(`${name}.parquet`, tempLocation);

  await run(sourcePaths as unknown as T, tempOutputParquet);

  await ValidateSchemaCommand.handler({
    concurrency: opts.concurrency,
    schema,
    paths: [tempOutputParquet],
    decodeGeometry: false,
  });

  const parquetStats = await parquetToStac(tempOutputParquet);

  const sw = new StacCollectionWriter('data', name);

  sw.asset('parquet', tempOutputParquet, {
    href: `./${name}.parquet`,
    type: 'application/vnd.apache.parquet',
    roles: ['data'],
    ...parquetStats.table,
  });

  for (const source of sources) {
    sw.collection.links.push({ rel: 'derived_from', href: source.href });
  }
  sw.collection.extent = parquetStats.extent;

  const collections = await sw.write(rootCatalog, q);

  await StacUpdater.collections(rootCatalog, [collections], true);
}
