import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import {
  createFileStats,
  createStacCatalog,
  createStacCollection,
  createStacItem,
} from '@topographic-system/shared/src/stac.ts';
import { UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, flag, option, optional, string } from 'cmd-ts';
import { parse } from 'path';
import type { StacAsset, StacCatalog, StacItem } from 'stac-ts';

function getAssetType(filename: string): string {
  if (filename.endsWith('.qgs')) return 'application/vnd.qgis.qgs+xml';
  else if (filename.endsWith('.png')) return 'image/png';
  else if (filename.endsWith('.tif') || filename.endsWith('.tiff')) return 'image/tiff';
  else if (filename.endsWith('.pdf')) return 'application/pdf';
  else return 'application/octet-stream';
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
    description: 'Target s3 location to deploy the files.',
  }),
  tag: option({
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

    // Find all the qgs files from the path
    const files = await fsa.toArray(fsa.list(args.project));
    const stacItems: Map<string, StacItem> = new Map();
    for (const file of files) {
      if (file.href.endsWith('.qgs')) {
        const splits = file.href.split('/');
        const projectName = parse(file.pathname).name; // example "nz-topo50-map"
        const projectSeries = splits[splits.length - 2]; // example "nztopo50map"
        if (projectName == null || projectSeries == null) {
          throw new Error(`Deploy: Invalid project file path ${file.href}`);
        }
        if (stacItems.has(projectSeries)) {
          throw new Error(`Multiple projects ${projectSeries} found at ${projectSeries} folder.`);
        }

        // Upload the QGS file to target location
        const targetPath = new URL(`${args.tag}/${projectSeries}/${projectName}.qgs`, args.target);
        if (args.commit) {
          logger.info({ source: file.href, destination: targetPath }, 'Deploy: Upload QGS File');
          const stream = fsa.readStream(file);
          await fsa.write(targetPath, stream, {
            contentType: 'application/vnd.qgis.qgs+xml',
          });
        }

        // Prepare data assets for stac item
        const data = await fsa.read(file);
        const assets: Record<string, StacAsset> = {
          extent: {
            href: targetPath.href,
            type: 'application/vnd.qgis.qgs+xml',
            roles: ['data'],
            ...createFileStats(data),
          } as StacAsset,
        };

        // Create Stac Item for the QGS file
        const stacItemPath = new URL(`${args.tag}/${projectSeries}/${projectName}.json`, args.target);
        logger.info({ source: file.href, destination: stacItemPath.href }, 'Deploy: Create Stac Item');
        // Add derived_from githash stac if provided
        const stacItemLinks = [];
        if (args.githash) {
          const sourcedStacItem = new URL(`${args.githash}/${projectSeries}/${projectName}.json`, args.target);
          if (await fsa.exists(sourcedStacItem)) {
            stacItemLinks.push({
              rel: 'derived_from',
              href: sourcedStacItem.href,
              type: 'application/json',
            });
          } else {
            throw new Error(`Deploy: Source stac item not found at ${sourcedStacItem.href}`);
          }
        }

        // Found and deploy all the assets file for the project
        const projectFolder = new URL(`/${projectSeries}/`, args.project);
        const projectFiles = await fsa.toArray(fsa.list(projectFolder));
        for (const file of projectFiles) {
          const filename = file.pathname.split('/').pop();
          if (!filename) throw new Error(`Deploy: Invalid file path ${file.href}`);
          if (filename.endsWith('.qgs')) continue; // Skip project file itself
          const assetTargetPath = new URL(`${args.tag}/${projectSeries}/${filename}`, args.target);
          // Upload asset file
          if (args.commit) {
            logger.info({ source: file.href, destination: assetTargetPath.href }, 'Deploy: Upload Asset File');
            const stream = fsa.readStream(file);
            await fsa.write(assetTargetPath, stream);
          }

          // Prepare assets for stac item
          const data = await fsa.read(file);
          assets[filename.split('.').pop() || filename] = {
            href: targetPath.href,
            type: getAssetType(filename),
            roles: ['graphic'],
            ...createFileStats(data),
          };
        }

        const item = createStacItem(projectName, stacItemLinks, assets);
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
        const sourcedCollection = new URL(`${args.githash}/${series}/collection.json`, args.target);
        collectionLinks.push({
          rel: 'derived_from',
          href: sourcedCollection.href,
          type: 'application/json',
        });
      }
      const description = `LINZ Topographic QGIS Project Series ${series}.`;
      const collection = createStacCollection(description, [], collectionLinks);
      catalogLinks.push({
        rel: 'collection',
        href: `./${series}/collection.json`,
        type: 'application/json',
      });
      if (args.commit) {
        const stacCollectionPath = new URL(`${args.tag}/${series}/collection.json`, args.target);
        logger.info({ mapSeries: series, destination: stacCollectionPath.href }, 'Deploy: Upload Stac Collections');
        await fsa.write(stacCollectionPath, JSON.stringify(collection, null, 2));
      }
    }

    logger.info({ project: args.project }, 'Deploy: Create Stac Catalog');
    const catalogPath = new URL(`${args.tag}/catalog.json`, args.target);
    const title = 'Topographic System QGIS Projects';
    const description = 'Topographic System QGIS Projects for generating maps.';

    let catalog = createStacCatalog(title, description, catalogLinks);
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
