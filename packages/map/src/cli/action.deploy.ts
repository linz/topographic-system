import { PassThrough } from 'node:stream';
import { basename } from 'path';

import { fsa } from '@chunkd/fs';
import {
  createFileStats,
  createStacCatalog,
  createStacCollection,
  createStacItem,
  getDataFromCatalog,
  logger,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { command, flag, option, optional, string } from 'cmd-ts';
import type { StacAsset, StacCatalog, StacItem } from 'stac-ts';
import tar from 'tar-stream';

import { pyRunner } from '../python.runner.ts';

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
    description: 'Tag to apply to the deployed items, could be githash, release version, etc.',
  }),
  githash: option({
    type: optional(string),
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

export const deployCommand = command({
  name: 'deploy',
  description: 'Deploy all the qgs project files and assets into target s3 location.',
  args: DeployArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project, commit: args.commit }, 'Deploy: Started');

    const rootCatalog = new URL('catalog.json', args.target);
    // TODO should it be called "/qgis/" ?
    const qgisCatalog = new URL('./qgis/catalog.json', rootCatalog);

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
        const targetPath = new URL(`${args.deployTag}/${projectSeries}/${projectName}.qgs`, qgisCatalog);
        if (args.commit) {
          logger.info({ source: file.href, destination: targetPath }, 'Deploy: Upload QGS File');
          const stream = fsa.readStream(file);
          await fsa.write(targetPath, stream, {
            contentType: 'application/vnd.qgis.qgs+xml',
          });
        }

        // Found and deploy all the assets file for the project as a tar file
        const projectFolder = new URL(`${projectSeries}/`, args.project);
        const targetAssetPath = new URL(`${args.deployTag}/${projectSeries}/${projectName}.tar`, qgisCatalog);
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

        // Add derived_from githash stac if provided
        if (args.githash) {
          const sourcedStacItem = new URL(`${args.githash}/${projectSeries}/${projectName}.json`, qgisCatalog);
          if (await fsa.exists(sourcedStacItem)) {
            stacItemLinks.push({
              rel: 'derived_from',
              href: sourcedStacItem.href,
              type: 'application/json',
            });
          } else {
            throw new Error(`Multiple projects found in ${projectSeries} folder.`);
          }
        }

        // Create Stac Item for the QGS file
        const stacItemPath = new URL(`${args.deployTag}/${projectSeries}/${projectName}.json`, qgisCatalog);
        logger.info({ source: file.href, destination: stacItemPath.href }, 'Deploy: Create Stac Item');
        const item = createStacItem(rootCatalog, projectName, stacItemLinks, assets);
        stacItems.set(projectSeries, item);
        if (args.commit) {
          logger.info({ source: file.href, destination: stacItemPath.href }, 'Deploy: Upload Stac Item File');
          await fsa.write(stacItemPath, JSON.stringify(item, null, 2));
        }
      }
    }

    if (stacItems.size === 0) throw new Error(`Deploy: No QGS project files found in ${args.project.href}`);

    const catalogLinks = [];
    for (const [series, item] of stacItems) {
      logger.info({ mapSeries: series }, 'Deploy: Create Stac Collection');
      const collectionLinks = [];
      collectionLinks.push({
        rel: 'item',
        href: `./${item.id}.json`,
        type: 'application/json',
      });
      if (args.githash) {
        const sourcedCollection = new URL(`${args.githash}/${series}/collection.json`, qgisCatalog);
        collectionLinks.push({
          rel: 'derived_from',
          href: sourcedCollection.href,
          type: 'application/json',
        });
      }
      const description = `LINZ Topographic QGIS Project Series ${series}.`;
      const collection = createStacCollection(rootCatalog, description, [], collectionLinks);
      catalogLinks.push({
        rel: 'collection',
        href: `./${series}/collection.json`,
        type: 'application/json',
      });
      if (args.commit) {
        const stacCollectionPath = new URL(`${args.deployTag}/${series}/collection.json`, qgisCatalog);
        logger.info({ mapSeries: series, destination: stacCollectionPath.href }, 'Deploy: Upload Stac Collections');
        await fsa.write(stacCollectionPath, JSON.stringify(collection, null, 2));
      }
    }

    logger.info({ project: args.project }, 'Deploy: Create Stac Catalog');
    const catalogPath = new URL(`${args.deployTag}/catalog.json`, qgisCatalog);
    const title = 'Topographic System QGIS Projects';
    const description = 'Topographic System QGIS Projects for generating maps.';

    let catalog = createStacCatalog(rootCatalog, title, description, catalogLinks);
    if (args.commit) {
      // Only try to update the catalog if committing
      const existing = await fsa.exists(catalogPath);
      if (existing) {
        catalog = await fsa.readJson<StacCatalog>(catalogPath);
        for (const series of stacItems.keys()) {
          if (catalog.links.find((link) => link.href === `./${series}/collection.json`)) continue;
          // Push new series collection link if not exists
          catalog.links.push({
            rel: 'collection',
            href: `./${series}/collection.json`,
            type: 'application/json',
          });
        }
        logger.info({ destination: catalogPath.href }, 'Deploy: Upload Stac Catalog File');
      }
      await fsa.write(catalogPath, JSON.stringify(catalog, null, 2));
    }

    logger.info({ project: args.project, commit: args.commit ? 'Uploaded' : 'Dry Run' }, 'Deploy: Finished');
  },
});
