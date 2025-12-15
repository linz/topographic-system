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
import { getGeometry } from '../python.runner.ts';
import { parse } from 'path';
import type { StacAsset, StacCatalog, StacItem } from 'stac-ts';
import { type BoundingBox, Bounds } from '@basemaps/geo';
import { downloadFiles } from './action.download.ts';

export const listMapSheetsArgs = {
  source: option({
    type: UrlFolder,
    long: 'source',
    description: 'Path or s3 of source parquet vector layers for loading the qgs file.',
  }),
  project: option({
    type: UrlFolder,
    long: 'project',
    description: 'Path that contains QGIS Project to deploy.',
  }),
  assets: option({
    type: optional(UrlFolder),
    long: 'assets',
    description: 'Path that contains assets to deploy.',
  }),
  target: option({
    type: string,
    long: 'target',
    description: 'Target directory or s3 bucket to write the mapsheet json file.',
  }),
  tag: option({
    type: string,
    long: 'tag',
    description: 'Tag to apply to the deployed items, could be githash, release version, etc.',
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
  args: listMapSheetsArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project, commit: args.commit }, 'Deploy: Started');

    // Download source files if not exists
    await downloadFiles(args.source);

    // File all the qgs files from the path
    const files = await fsa.toArray(fsa.list(args.project));
    const stacItems: Map<string, StacItem[]> = new Map();
    for (const file of files) {
      if (file.href.endsWith('.qgs')) {
        const splits = file.href.split('/');
        const projectName = parse(args.project.pathname).name;
        const projectSeries = splits[splits.length - 2];
        if (projectName == null || projectSeries == null) {
          throw new Error(`Deploy: Invalid project file path ${file.href}`);
        }

        if (args.commit) {
          const targetPath = new URL(`${args.tag}/${projectSeries}/${projectName}.qgs`, args.target);
          logger.info({ source: file.href, destination: targetPath }, 'Deploy: Upload QGS File');
          const stream = fsa.readStream(file);
          await fsa.write(targetPath, stream, {
            contentType: 'application/vnd.qgis.qgs+xml',
          });
        }

        // Create Stac Item for the QGS file
        const geometry = await getGeometry(file);
        const links = [
          { href: `./${projectName}.json`, rel: 'self' },
          { href: './collection.json', rel: 'collection', type: 'application/json' },
          { href: './collection.json', rel: 'parent', type: 'application/json' },
        ];
        const data = await fsa.read(file);
        const assets = {
          extent: {
            href: `./${projectName}.qgs`,
            type: 'application/vnd.qgis.qgs+xml',
            roles: ['data'],
            ...createFileStats(data),
          } as StacAsset,
        };

        const stacItemPath = new URL(`${args.tag}/${projectSeries}/${projectName}.json`, args.target);
        logger.info({ source: file.href, destination: stacItemPath.href }, 'Deploy: Create Stac Item');
        const item = createStacItem(projectName, geometry.geometry, geometry.bbox, );
        if (stacItems.has(projectSeries) === false) {
          stacItems.set(projectSeries, [item]);
        } else {
          stacItems.get(projectSeries)?.push(item);
        }
        if (args.commit) {
          logger.info({ source: file.href, destination: stacItemPath.href }, 'Deploy: Upload Stac Item File');
          await fsa.write(stacItemPath, JSON.stringify(item, null, 2));
        }
      }
    }

    for (const [series, items] of stacItems) {
      logger.info({ mapSeries: series }, 'Deploy: Create Stac Collection');
      const allBbox: BoundingBox[] = [];
      const links = [{ href: '../catalog.json', rel: 'parent', type: 'application/json' }];
      for (const item of items) {
        if (item.bbox) allBbox.push(Bounds.fromBbox(item.bbox));
        links.push({
          rel: 'item',
          href: `./${item.id}.json`,
          type: 'application/json',
        });
      }
      const description = `LINZ Topographic Qgis Project Series ${series}.`;
      const bbox = Bounds.union(allBbox).toBbox();

      const collection = createStacCollection(description, bbox, links);
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
    let catalog = createStacCatalog(title, description);
    const existing = await fsa.exists(catalogPath);
    if (existing) {
      catalog = await fsa.readJson<StacCatalog>(catalogPath);
      for (const series of stacItems.keys()) {
        catalog.links.push({
          rel: 'collection',
          href: `./${series}/collection.json`,
          type: 'application/json',
        });
      }
    }

    if (args.commit) {
      logger.info({ destination: catalogPath.href }, 'Deploy: Upload Stac Catalog File');
      await fsa.write(catalogPath, JSON.stringify(catalog, null, 2));
    }

    logger.info({ project: args.project, commit: args.commit ? 'Uploaded' : 'Dry Run' }, 'Deploy: Finished');
  },
});
