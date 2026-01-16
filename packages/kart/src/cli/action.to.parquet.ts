import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { createFileStats, RootCatalogFile } from '@topographic-system/shared/src/stac.ts';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import os from 'os';
import { basename } from 'path';
import { StacAsset, StacCatalog, StacCollection, StacItem } from 'stac-ts';
import { $ } from 'zx';

const Concurrency = os.cpus().length;
const CommonDateTime = new Date();
const Q = new ConcurrentQueue(Concurrency);
const MediaTypes = {
  parquet: 'application/vnd.apache.parquet',
  geojson: 'application/geo+json',
  json: 'application/json',
  gpkg: 'application/geopackage+sqlite3',
  '': 'application/octet-stream',
};
const Roles = {
  parquet: 'data',
  geojson: 'data',
  json: 'metadata',
  gpkg: 'data',
  '': 'data',
};

async function upsertItemToCollection(
  stacItemFile: URL,
  stacCollectionFile?: URL,
  stacCatalogFile?: URL,
): Promise<URL> {
  if (!stacCollectionFile) {
    stacCollectionFile = new URL('./collection.json', stacItemFile);
  }
  if (!stacCatalogFile) {
    stacCatalogFile = new URL('../catalog.json', stacCollectionFile);
  }
  let stacCollection = <StacCollection>{
    type: 'Collection',
    stac_version: '1.0.0',
    id: await readOrCreateCollectionId(stacCollectionFile),
    features: [],
    description: `Collection of ${urlToTitle(stacCollectionFile)}`,
    extent: {
      spatial: {
        bbox: [[]],
      },
      temporal: {
        interval: [[CommonDateTime.toISOString(), CommonDateTime.toISOString()]],
      },
    },
    links: [
      { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
      { rel: 'self', href: stacCollectionFile.href, type: 'application/json' },
      { rel: 'parent', href: stacCatalogFile.href, type: 'application/json' },
    ],
    license: 'CC-BY-4.0',
    created: CommonDateTime.toISOString(),
    updated: CommonDateTime.toISOString(),
  };
  if (await fsa.exists(stacCollectionFile)) {
    stacCollection = await fsa.readJson<StacCollection>(stacCollectionFile);
  }
  const oldLinkToItem = stacCollection.links.find((link) => link.href === stacItemFile.href && link.rel === 'item');
  if (oldLinkToItem) {
    logger.warn(
      {
        itemHref: stacItemFile.href,
        collectionHref: stacCollectionFile.href,
        stacCollectionExtent: stacCollection.extent,
      },
      `STAC Collection already contains link to item. Existing extents will not be updated.`,
    );
    return stacCollectionFile;
  }
  stacCollection.links.push({
    href: stacItemFile.href,
    rel: 'item',
    type: 'application/geo+json',
  });
  stacCollection['updated'] = CommonDateTime.toISOString();
  const stacItem = await fsa.readJson<StacItem>(stacItemFile);
  if (stacItem.bbox) {
    stacCollection.extent.spatial.bbox.push(stacItem.bbox);
  }
  if (stacItem.properties.start_datetime && stacItem.properties.end_datetime) {
    stacCollection.extent.temporal.interval.push([
      stacItem.properties.start_datetime,
      stacItem.properties.end_datetime,
    ]);
  }
  await fsa.write(stacCollectionFile, JSON.stringify(stacCollection, null, 2));
  logger.info({ stacCollectionFile: stacCollectionFile.href }, 'ToParquet:STACItemToCollectionUpserted');
  await upsertCollectionToCatalog(stacCollectionFile, stacCatalogFile);

  return stacCollectionFile;
}

/**
 * Given a data asset path, create or update the corresponding STAC Item with the asset information.
 * Note:
 * One STAC Item may contain multiple assets.
 * Running this function multiple times with different assets for the same dataset will update the same STAC Item.
 * Running this in parallel for different assets of the same dataset may lead to race conditions,
 * as STAC Collection and Catalogs up to the RootCatalog are updated.
 *
 * @param dataAssetPath - The URL of the data asset to be added to the STAC Item.
 * @param stacItemFile - Optional URL of the STAC Item file. If not provided, it will be derived from the data asset path.
 * @param stacCollectionFile - Optional URL of the STAC Collection file. If not provided, it will be derived from the data asset path.
 * @returns The updated or newly created STAC Item, which includes the new asset and has been saved to s3.
 * */
async function upsertAssetToItem(dataAssetPath: URL, stacItemFile?: URL, stacCollectionFile?: URL): Promise<URL> {
  const extension = dataAssetPath.href.split('.').pop() || '';
  const dataset = basename(dataAssetPath.href, extension);
  if (!stacCollectionFile) {
    stacCollectionFile = new URL('./collection.json', dataAssetPath);
  }
  if (!stacItemFile) {
    stacItemFile = new URL(`./${dataset}.json`, dataAssetPath);
  }

  const datatype = (extension in MediaTypes ? extension : '') as keyof typeof MediaTypes;
  const currentDate = CommonDateTime.toISOString();

  const assetStats = createFileStats(await fsa.read(dataAssetPath));
  const stacAsset: StacAsset = {
    href: dataAssetPath.href,
    title: `${dataset}`,
    description: `${dataset} data in ${extension} format`,
    type: MediaTypes[datatype],
    roles: [Roles[datatype]],
    ...assetStats,
  };
  let stacItem: StacItem = {
    type: 'Feature',
    stac_version: '1.0.0',
    stac_extensions: [],
    id: dataset,
    geometry: null, // defines the full footprint of the asset represented by the STAC Item
    bbox: [], // Bounding Box of the asset represented by this Item, formatted according to RFC 7946, section 5. https://tools.ietf.org/html/rfc7946#section-5
    properties: {
      datetime: currentDate,
    },
    links: [
      { href: stacItemFile.href, rel: 'self' },
      { href: stacCollectionFile.href, rel: 'collection', type: 'application/json' },
      { href: './collection.json', rel: 'parent', type: 'application/json' },
      { href: RootCatalogFile.href, rel: 'root', type: 'application/json' },
    ],
    assets: {},
    collection: '',
  };

  if (await fsa.exists(stacItemFile)) {
    stacItem = await fsa.readJson<StacItem>(stacItemFile);
    if (compareStacAssets(stacItem.assets[extension], stacAsset)) {
      logger.info({ dataset, stacItemFile: stacItemFile.href }, 'ToParquet:STACItemAlreadyUpToDate');
      return stacItemFile;
    }
    logger.info({ dataset, stacItemFile: stacItemFile.href }, 'ToParquet:STACItemUpdateNeeded');
  } else {
    logger.info({ dataset, stacItemFile: stacItemFile.href }, 'ToParquet:STACItemCreateNeeded');
  }
  stacItem.assets[extension] = stacAsset;

  stacCollectionFile = await upsertItemToCollection(stacItemFile, stacCollectionFile);
  stacItem.collection = await readOrCreateCollectionId(stacCollectionFile);
  await fsa.write(stacItemFile, JSON.stringify(stacItem, null, 2));
  logger.info({ stacItemFile: stacItemFile.href }, 'ToParquet:STACAssetUpserted');
  return stacItemFile;
}

function compareStacAssets(a: StacAsset | undefined, b: StacAsset | undefined): boolean {
  if (a && b)
    return (
      a.href === b.href &&
      a.type === b.type &&
      a['file:checksum'] === b['file:checksum'] &&
      a['file:size'] === b['file:size']
    );
  return false;
}

async function readOrCreateCollectionId(stacCollectionFile: URL): Promise<string> {
  if (await fsa.exists(stacCollectionFile)) {
    const collection = await fsa.readJson<StacCollection>(stacCollectionFile);
    if (collection.id) {
      return collection.id;
    }
  }
  const timestamp = CommonDateTime.toISOString().replace(/[-:.]/g, '').slice(0, 15);
  return `linz-collection-${stacCollectionFile.pathname.replaceAll('/', '-')}-${timestamp}`;
}

async function readOrCreateCatalogId(stacCatalogFile: URL): Promise<string> {
  if (await fsa.exists(stacCatalogFile)) {
    const catalog = await fsa.readJson<StacCatalog>(stacCatalogFile);
    if (catalog.id) {
      return catalog.id;
    }
  }
  return `linz-catalog-${stacCatalogFile.pathname.replaceAll('/', '-')}`;
}

function determineS3AssetLocation(dataset: string, output: string, tag?: string): URL {
  logger.debug({ zx_env: $.env });
  const repo = ($.env['GITHUB_REPOSITORY'] || 'unknown').split('/')[1];
  if (!tag) {
    if (is_merge_to_master() || is_release()) {
      // add version number and "next" not "latest" ?
      tag = `${CommonDateTime.getFullYear()}/${CommonDateTime.toISOString().split('T')[0]}`;
      // tag = `${new Date().toISOString()}`;
    } else if (is_pr()) {
      const ref = $.env['GITHUB_REF_NAME'] || '';
      const prMatch = ref.match(/(\d+)\/merge/);
      if (prMatch) {
        tag = `pr-${prMatch[0]}`;
      } else {
        tag = `pr-unknown`;
      }
    } else {
      tag = 'dev';
    }
  }
  logger.info(
    { repo, tag, master: is_merge_to_master(), release: is_release(), pr: is_pr() },
    'DetermineS3Location:Context',
  );
  return new URL(`topo/🚧/${repo}/${dataset}/${tag}/${basename(output)}`, RootCatalogFile);
}

function is_release(): boolean {
  // merge target is master, and the workflow is release
  const workflow = $.env['GITHUB_WORKFLOW_REF'] || '';
  return is_merge_to_master() && workflow.toLowerCase().includes('release');
}

function is_pr(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return ref.startsWith('refs/pull/');
}

function is_merge_to_master(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return !is_pr() && ref.endsWith('/master');
}

async function upsertSubCatalogToCatalog(stacSubCatalogFile: URL, stacCatalogFile?: URL): Promise<URL> {
  if (!stacCatalogFile) {
    stacCatalogFile = new URL('../catalog.json', stacSubCatalogFile);
  }
  let stacCatalog = <StacCatalog>{
    type: 'Catalog',
    stac_version: '1.0.0',
    stac_extensions: [],
    id: await readOrCreateCatalogId(stacCatalogFile),
    title: `Catalog for ${urlToTitle(stacCatalogFile)}`,
    description: `Description of Catalog ${urlToTitle(stacCatalogFile)}`,
    links: [
      { rel: 'self', href: stacCatalogFile.href, type: 'application/json' },
      { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    ],
  };
  const parentCatalogFile = new URL('../catalog.json', stacCatalogFile);
  if (stacCatalogFile.href !== RootCatalogFile.href) {
    stacCatalog.links.push({ rel: 'parent', href: parentCatalogFile.href, type: 'application/json' });
  } else {
    stacCatalog.title = 'LINZ Topographic Data Catalog';
    stacCatalog.description = 'Catalog of LINZ Topographic Data assets.';
  }
  if (await fsa.exists(stacCatalogFile)) {
    stacCatalog = await fsa.readJson<StacCatalog>(stacCatalogFile);
    const oldLinkToCollection = stacCatalog.links.find(
      (link) => link.href === stacSubCatalogFile.href && link.rel === 'collection',
    );
    if (oldLinkToCollection) {
      logger.info(
        {
          collectionHref: stacSubCatalogFile.href,
          catalogHref: stacCatalogFile.href,
        },
        `STAC Catalog already contains link to collection, skipping addition.`,
      );
      return stacCatalogFile;
    }
  }
  stacCatalog.links.push({
    href: stacSubCatalogFile.href,
    rel: 'catalog',
    type: 'application/json',
  });
  await fsa.write(stacCatalogFile, JSON.stringify(stacCatalog, null, 2));
  logger.info({ stacCatalogFile: stacCatalogFile.href }, 'ToParquet:STACSubCatalogToCatalogUpserted');
  if (stacCatalogFile.href !== RootCatalogFile.href) {
    await upsertSubCatalogToCatalog(stacCatalogFile, parentCatalogFile);
  }
  return stacCatalogFile;
}

function urlToTitle(fileName: URL): string {
  return fileName.pathname.replaceAll('/', ' ').replaceAll('_', ' ');
}

async function upsertCollectionToCatalog(stacCollectionFile: URL, stacCatalogFile?: URL): Promise<URL> {
  if (!stacCatalogFile) {
    stacCatalogFile = new URL('../catalog.json', stacCollectionFile);
  }
  let stacCatalog = <StacCatalog>{
    type: 'Catalog',
    stac_version: '1.0.0',
    stac_extensions: [],
    id: await readOrCreateCatalogId(stacCatalogFile),
    title: `Catalog for ${urlToTitle(stacCatalogFile)}`,
    description: `Description of Catalog ${urlToTitle(stacCatalogFile)}`,
    links: [
      { rel: 'self', href: stacCatalogFile.href, type: 'application/json' },
      { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    ],
  };
  const parentCatalogFile = new URL('../catalog.json', stacCatalogFile);
  if (stacCatalogFile.href !== RootCatalogFile.href) {
    stacCatalog.links.push({ rel: 'parent', href: parentCatalogFile.href, type: 'application/json' });
  } else {
    stacCatalog.title = 'LINZ Topographic Data Catalog';
    stacCatalog.description = 'Catalog of LINZ Topographic Data assets.';
  }
  if (await fsa.exists(stacCatalogFile)) {
    stacCatalog = await fsa.readJson<StacCatalog>(stacCatalogFile);
    const oldLinkToCollection = stacCatalog.links.find(
      (link) => link.href === stacCollectionFile.href && link.rel === 'collection',
    );
    if (oldLinkToCollection) {
      logger.info(
        {
          collectionHref: stacCollectionFile.href,
          catalogHref: stacCatalogFile.href,
        },
        `STAC Catalog already contains link to collection, skipping addition.`,
      );
      return stacCatalogFile;
    }
  }
  stacCatalog.links.push({
    href: stacCollectionFile.href,
    rel: 'collection',
    type: 'application/json',
  });
  await fsa.write(stacCatalogFile, JSON.stringify(stacCatalog, null, 2));
  logger.info({ stacCatalogFile: stacCatalogFile.href }, 'ToParquet:STACCollectionToCatalogUpserted');
  await upsertSubCatalogToCatalog(stacCatalogFile, parentCatalogFile);
  return stacCatalogFile;
}

export const parquetCommand = command({
  name: 'to-parquet',
  description: 'Convert gpkg files in a folder to parquet format',
  args: {
    compression: option({
      type: optional(string),
      long: 'compression',
      description: 'compression type for parquet files (default: zstd)',
      defaultValue: () => 'zstd',
    }),
    compression_level: option({
      type: optional(number),
      long: 'compression-level',
      description: 'compression level for parquet files (default: 17)',
      defaultValue: () => 17,
    }),
    // Note: inverted logic due to bug/feature in cmd-ts flag defaults (flag not set always means false, regardless of defaultValue)
    no_sort_by_bbox: flag({
      type: boolean,
      defaultValue: () => false,
      long: 'no-sort-by-bbox',
      description: 'whether to _not_ sort parquet files by bounding box (default: false)',
    }),
    row_group_size: option({
      type: optional(number),
      long: 'row-group-size',
      description: 'row group size for parquet files (default: 4096)',
      defaultValue: () => 4096,
    }),
    sourceFiles: restPositionals({
      type: string,
      description: 'List of folders or files to convert (default: all .gpkg files in ./export)',
    }),
  },
  async handler(args) {
    registerFileSystem();

    logger.info(
      {
        concurrency: Concurrency,
        compression: args.compression,
        compression_level: args.compression_level,
        sort_by_bbox: !args.no_sort_by_bbox,
      },
      'ToParquet:Start',
    );

    const filesToProcess: string[] = [];
    const sourceFileArguments = args.sourceFiles.length > 0 ? args.sourceFiles : ['./export'];

    for (const sourceFileArgument of sourceFileArguments) {
      const sourcePath = fsa.toUrl(sourceFileArgument);
      const stat = await fsa.head(sourcePath);
      if (stat && stat.isDirectory) {
        const filePaths = await fsa.toArray(fsa.list(sourcePath, { recursive: true }));
        for (const filePath of filePaths) {
          if (filePath.href.endsWith('.gpkg')) {
            filesToProcess.push(filePath.pathname);
          }
        }
      } else if (stat) {
        if (sourcePath.href.endsWith('.gpkg')) {
          filesToProcess.push(sourcePath.pathname);
        }
      }
    }

    if (filesToProcess.length === 0) {
      logger.info('ToParquet:No files to process');
      return;
    }

    await $`mkdir ./parquet`;
    logger.info({ filesToProcess }, 'ToParquet:Processing');
    for (const file of filesToProcess) {
      Q.push(async () => {
        const dataset = basename(file, '.gpkg');
        const output = `./parquet/${dataset}.parquet`;
        const command = [
          'ogr2ogr',
          '-f',
          'Parquet',
          output,
          file,
          '-dsco',
          `COMPRESSION=${args.compression}`,
          '-dsco',
          `COMPRESSION_LEVEL=${args.compression_level}`,
          '-dsco',
          `ROW_GROUP_SIZE=${args.row_group_size}`,
        ];
        if (!args.no_sort_by_bbox) {
          command.push('-dsco', 'SORT_BY_BBOX=YES');
        }
        await $`${command}`;
        const assetFile = determineS3AssetLocation(dataset, output);
        logger.info({ assetFile }, 'ToParquet:UploadingParquet');
        await fsa.write(assetFile, fsa.readStream(fsa.toUrl(output)), {
          contentType: 'application/vnd.apache.parquet',
        });
        const stacItemFile = await upsertAssetToItem(assetFile);
        // const stacCollectionFile = await upsertItemToCollection(stacItemFile);
        if (is_merge_to_master()) {
          await upsertItemToCollection(stacItemFile, new URL('../../next/collection.json', stacItemFile));
        } else if (is_release()) {
          await upsertItemToCollection(stacItemFile, new URL('../../latest/collection.json', stacItemFile));
        }
        logger.info({ output, stacItemFile: stacItemFile.href }, 'ToParquet:Completed');
      });
    }

    await Q.join().catch((err: unknown) => {
      logger.fatal({ err }, 'ToParquet:Error');
      throw err;
    });
    logger.info('ToParquet:Completed');
  },
});
