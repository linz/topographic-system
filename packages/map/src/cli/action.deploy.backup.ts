import { PassThrough } from 'node:stream';
import { basename } from 'path';

import { fsa } from '@chunkd/fs';
import {
  createFileStats,
  createStacCollection,
  createStacItem,
  getDataFromCatalog,
  logger,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { command, flag, option, optional, string } from 'cmd-ts';
import type { StacAsset, StacItem } from 'stac-ts';
import tar from 'tar-stream';

import { pyRunner } from '../python.runner.ts';
import { createCatalog } from '../stac.ts';

async function deployAssetsAsTar(projectFolder: URL, tarTargetPath: URL, commit?: boolean): Promise<URL | null> {
  const tarPack = tar.pack();
  const pass = new PassThrough();
  tarPack.pipe(pass);

  const projectFiles = await fsa.toArray(fsa.list(projectFolder));
  if (projectFiles.length === 0) return null;
  let fileCount = 0;
  for (const file of projectFiles) {
    const filename = basename(file.href);
    if (!filename) throw new Error(`Deploy: Invalid file path ${file.href}`);
    if (filename.endsWith('.qgs')) continue;

    const data = await fsa.read(file);
    tarPack.entry({ name: filename, size: data.byteLength }, data);
    fileCount++;
  }
  if (fileCount === 0) return null;

  tarPack.finalize();

  if (commit) {
    await fsa.write(tarTargetPath, pass, {
      contentType: 'application/x-tar',
    });

    logger.info({ destination: tarTargetPath.href }, 'Deploy: Upload Tar Asset File');
  }

  return tarTargetPath;
}

export const DeployArgs = {
  project: option({
    type: UrlFolder,
    long: 'project',
    description: 'Directory containing the QGIS Project to deploy.',
  }),
  target: option({
    type: UrlFolder,
    long: 'target',
    description: 'Target location to deploy the files. (eg "s3://linz-topographic/") ',
  }),
  source: option({
    type: Url,
    long: 'source',
    description: 'Source data catalog.json that contains the layers.',
  }),
  dataTag: option({
    type: optional(string),
    long: 'data-tag',
    defaultValue: () => 'latest',
    defaultValueIsSerializable: true,
    description: 'data tag to use when looking for source layers. Default to latest if not provided.',
  }),
  deployTag: option({
    type: string,
    long: 'tag',
    description: 'Tag to apply to the deployed items, could be pull request, release version, etc.',
  }),
  githash: option({
    type: string,
    long: 'githash',
    description: 'Github hash to tag the deployment with.',
  }),
  commit: flag({
    long: 'commit',
    description: 'Actually start the import',
    defaultValue: () => false,
    defaultValueIsSerializable: true,
  }),
};

export const DeployCommand = command({
  name: 'deploy',
  description: 'Deploy all the qgs project files and assets into target s3 location.',
  args: DeployArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project, commit: args.commit }, 'Deploy: Started');

    const rootCatalogPath = new URL('catalog.json', args.target);
    // TODO should it be called "/qgis/" ?
    const qgisCatalogPath = new URL('./qgis/catalog.json', rootCatalogPath);
    const commitTag = `commit_prefix=${args.githash.charAt(0)}/commit=${args.githash}`;

    // Find all the qgs files from the path
    const files = await fsa.toArray(fsa.list(args.project));
    const stacItems: Map<string, StacItem> = new Map();
    for (const file of files) {
      if (file.href.endsWith('.qgs')) {
        const splits = file.href.split('/');
        const projectName = basename(file.href, '.qgs'); // example "nz-topo50-map"
        const projectSeries = splits[splits.length - 2]; // example "nztopo50map"
        if (projectName == null || projectSeries == null) {
          throw new Error(`Deploy: Invalid project file path ${file.href}`);
        }
        if (stacItems.has(projectSeries)) {
          throw new Error(`Multiple projects ${projectSeries} found at ${projectSeries} folder.`);
        }

        // Find all the source layers for project
        const layers = await pyRunner.listSourceLayers(file);
        if (layers.length === 0) throw new Error(`No source layers found in project ${file.href}`);
        // Prepare source layer links for stac item
        const stacItemLinks = [];
        for (const layer of layers) {
          const layerCollection = await getDataFromCatalog(args.source, layer, args.dataTag);
          stacItemLinks.push({
            rel: 'dataset',
            href: layerCollection.href,
            type: 'application/json',
          });
        }

        // Upload the QGS file to target location
        const targetPath = new URL(`${projectSeries}/${commitTag}/${projectName}.qgs`, qgisCatalogPath);
        if (args.commit) {
          logger.info({ source: file.href, destination: targetPath }, 'Deploy: Upload QGS File');
          const stream = fsa.readStream(file);
          await fsa.write(targetPath, stream, {
            contentType: 'application/vnd.qgis.qgs+xml',
          });
        }

        // Found and deploy all the assets file for the project as a tar file
        const projectFolder = new URL(`${projectSeries}/`, args.project);
        const targetAssetPath = new URL(`${projectSeries}/${commitTag}/${projectName}.tar`, qgisCatalogPath);
        const assetLocation = await deployAssetsAsTar(projectFolder, targetAssetPath, args.commit);
        if (assetLocation) {
          stacItemLinks.push({
            rel: 'assets',
            href: assetLocation.href,
            type: 'application/x-tar',
          });
        }

        // Prepare data assets for stac item
        const assets: Record<string, StacAsset> = {
          project: {
            href: targetPath.href,
            type: 'application/vnd.qgis.qgs+xml',
            roles: ['data'],
            ...(await createFileStats(file)),
          } as StacAsset,
        };

        // Create Stac Item for the QGS file
        const stacItemPath = new URL(`${projectSeries}/${commitTag}/${projectName}.json`, qgisCatalogPath);
        const derivedStacItemPath = new URL(`${projectSeries}/${args.deployTag}/${projectName}.json`, qgisCatalogPath);
        logger.info({ source: file.href, destination: stacItemPath.href }, 'Deploy: Create Stac Item');
        const item = createStacItem(rootCatalogPath, projectName, stacItemLinks, assets);
        const derivedStacItem = {
          ...item,
          id: `${args.deployTag}-${item.id}`,
          links: [...item.links, { rel: 'derived_from', href: stacItemPath.href, type: 'application/json' }],
        };
        stacItems.set(projectSeries, item);
        if (args.commit) {
          // Upload stac item for the project
          logger.info({ source: file.href, destination: stacItemPath.href }, 'Deploy: Upload Stac Item');
          await fsa.write(stacItemPath, JSON.stringify(item, null, 2));
          logger.info({ source: file.href, destination: derivedStacItemPath.href }, 'Deploy: Upload Derived Stac Item');
          await fsa.write(derivedStacItemPath, JSON.stringify(derivedStacItem, null, 2));
        }
      }
    }

    if (stacItems.size === 0) throw new Error(`Deploy: No QGS project files found in ${args.project.href}`);

    const qgisCatalogLinks = [];
    for (const [series, item] of stacItems) {
      logger.info({ mapSeries: series }, 'Deploy: Create Stac Collection');
      const collectionLinks = [];
      collectionLinks.push({
        rel: 'item',
        href: `./${item.id}.json`,
        type: 'application/json',
      });
      const title = `Topographic System QGIS ${series} Projects`;
      const description = `LINZ Topographic QGIS Project Series ${series}.`;
      const collection = createStacCollection(rootCatalogPath, description, [], collectionLinks);
      if (args.commit) {
        // Upload stac collections
        const stacCollectionPath = new URL(`${series}/${commitTag}/collection.json`, qgisCatalogPath);
        logger.info({ mapSeries: series, destination: stacCollectionPath.href }, 'Deploy: Upload Stac Collections');
        await fsa.write(stacCollectionPath, JSON.stringify(collection, null, 2));
        const derivedCollectionPath = new URL(`${series}/${args.deployTag}/collection.json`, qgisCatalogPath);
        const derivedCollection = {
          ...collection,
          id: `${args.deployTag}-${collection.id}`,
          links: [
            ...collection.links,
            { rel: 'derived_from', href: stacCollectionPath.href, type: 'application/json' },
          ],
        };
        logger.info(
          { mapSeries: series, destination: derivedCollectionPath.href },
          'Deploy: Upload Derived Stac Collections',
        );
        await fsa.write(derivedCollectionPath, JSON.stringify(derivedCollection, null, 2));

        // Create and upload catalog for project series
        const seriesCatalogPath = new URL(`${series}/catalog.json`, qgisCatalogPath);
        const catalogLinks = [
          {
            rel: 'collection',
            href: `./${args.deployTag}/collection.json`,
            type: 'application/json',
          },
          {
            rel: 'collection',
            href: `./${commitTag}/collection.json`,
            type: 'application/json',
          },
        ];
        const catalog = await createCatalog(seriesCatalogPath, rootCatalogPath, title, description, catalogLinks);
        logger.info({ destination: seriesCatalogPath.href }, 'Deploy: Upload Stac Catalog File');
        await fsa.write(seriesCatalogPath, JSON.stringify(catalog, null, 2));
        qgisCatalogLinks.push({
          rel: 'child',
          href: `./${series}/catalog.json`,
          type: 'application/json',
        });
      }
    }

    logger.info({ project: args.project }, 'Deploy: Create Stac Catalog');
    const title = 'Topographic System QGIS Projects';
    const description = 'Topographic System QGIS Projects for generating maps.';
    const qgisCatalog = await createCatalog(qgisCatalogPath, rootCatalogPath, title, description, qgisCatalogLinks);
    if (args.commit) {
      logger.info({ destination: qgisCatalogPath.href }, 'Deploy: Upload Stac Catalog File');
      await fsa.write(qgisCatalogPath, JSON.stringify(qgisCatalog, null, 2));
      // Update root catalog
      const link = {
        rel: 'child',
        href: `./qgis/catalog.json`,
        type: 'application/json',
      };
      const catalog = await createCatalog(
        rootCatalogPath,
        rootCatalogPath,
        'LINZ Topographic System',
        'LINZ Topographic System Catalog',
        [link],
      );
      logger.info({ destination: rootCatalogPath.href }, 'Deploy: Update Root Catalog File');
      await fsa.write(rootCatalogPath, JSON.stringify(catalog, null, 2));
    }

    logger.info({ project: args.project, commit: args.commit ? 'Uploaded' : 'Dry Run' }, 'Deploy: Finished');
  },
});
