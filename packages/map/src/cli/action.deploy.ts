import { basename } from 'path';
import { zstdCompressSync } from 'zlib';

import { fsa } from '@chunkd/fs';
import {
  concurrency,
  getDataFromCatalog,
  logger,
  qFromArgs,
  registerFileSystem,
  Url,
  UrlFolder,
  UrlFolders,
} from '@linzjs/topographic-system-shared';
import { StacCollectionWriter, StacGeometry, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, multioption, option, optional, restPositionals } from 'cmd-ts';
import type { LimitFunction } from 'p-limit';
import type { StacCollection } from 'stac-ts';
import tar from 'tar-stream';

import { getQgisProjectMeta } from '../qgis.ts';

async function buildTarBuffer(...folders: URL[]): Promise<Buffer | null> {
  const tarPack = tar.pack();
  const chunks: Buffer[] = [];

  tarPack.on('data', (chunk) => chunks.push(Buffer.from(chunk)));

  let fileCount = 0;

  for (const folder of folders) {
    const projectFiles = await fsa.toArray(fsa.list(folder));
    if (projectFiles.length === 0) continue;

    for (const file of projectFiles) {
      const filename = basename(file.pathname);
      if (!filename) throw new Error(`Deploy: Invalid file path ${file.href}`);
      if (filename.endsWith('.tar')) continue; // TODO
      if (filename.endsWith('.qgs')) continue;

      const data = await fsa.read(file);
      logger.info({ filename, size: data.byteLength }, 'Tar:Pack');
      tarPack.entry({ name: filename, size: data.byteLength }, data);
      fileCount++;
    }
  }
  if (fileCount === 0) return null;

  tarPack.finalize();

  await new Promise<void>((resolve, reject) => {
    tarPack.on('end', resolve);
    tarPack.on('error', reject);
  });

  const before = Buffer.concat(chunks);
  const compressed = zstdCompressSync(before);
  logger.info(
    { fileCount, compressed: compressed.byteLength, ratio: before.byteLength / compressed.byteLength },
    'Tar:Packed',
  );
  return compressed;
}

async function deployProject(
  project: URL,
  args: {
    source: URL;
    target: URL;
    extras: URL[];
    dataTag?: string;
  },
  q: LimitFunction,
): Promise<URL> {
  const projectName = basename(project.href, '.qgs');
  const meta = await getQgisProjectMeta(project);
  if (meta.layers.length === 0) throw new Error(`No source layers found in project ${project.href}`);

  // QGIS may have duplicate layer sources, so get the unique sources
  const uniqueSource = new Set(meta.layers.map((layer) => layer.source.replace('.parquet', '').replace('.geojson', '')));

  const datasetLinks = await Promise.all(
    [...uniqueSource].map((layer) =>
      q(async () => {
        const collectionUrl = await getDataFromCatalog(
          args.source,
          layer,
        );
        const collection = await fsa.readJson<StacCollection>(collectionUrl);
        return { collection, url: collectionUrl };
      }),
    ),
  );
  const tarBuffer = await buildTarBuffer(new URL('.', project), ...args.extras);

  const sw = new StacCollectionWriter('qgis', projectName);
  sw.collection.title = `Topographic System QGIS ${projectName} Projects.`;
  sw.collection.description = `LINZ Topographic QGIS Project Series ${projectName}.`;

  const item = sw.item(projectName);

  for (const link of datasetLinks) {
    StacGeometry.extend(item, link.collection);
    item.links.push({ rel: 'dataset', href: link.url.href, type: 'application/json' });
  }

  sw.itemAsset(projectName, 'project', project, {
    href: `./${projectName}.qgs`,
    type: 'application/vnd.qgis.qgs+xml',
    roles: ['data'],
  });

  if (tarBuffer) {
    sw.itemAsset(projectName, 'assets', tarBuffer, {
      href: `./${projectName}.tar.zst`,
      type: 'application/zstd',
      roles: ['data'],
    });
  }

  logger.info({ source: project.href, destination: args.target }, 'Deploy: Create Commit Stac Item');
  return await sw.write(args.target, q);
}

export const DeployArgs = {
  concurrency,
  project: restPositionals({
    type: Url,
    description: 'QGIS Project to deploy.',
  }),
  extras: multioption({
    type: UrlFolders,
    long: 'extra-assets',
    description: 'Extra assets to be deployed',
  }),
  target: option({
    type: UrlFolder,
    long: 'target',
    description: 'Target location to deploy the files. (eg "s3://linz-topographic/") ',
  }),
  source: option({
    type: optional(Url),
    long: 'source',
    description: 'Source data catalog.json that contains the layers. defaults to target catalog',
  }),
};

export const DeployCommand = command({
  name: 'deploy',
  description: 'Deploy all the qgs project files and assets into target s3 location.',
  args: DeployArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project }, 'Deploy: Started');
    const q = qFromArgs(args);

    const rootCatalog = new URL('catalog.json', args.target);

    const collections = new Set<URL>();

    for (const proj of args.project) {
      if (!proj.href.endsWith('.qgs')) throw new Error(`${proj.href} needs to end with .qgs`);

      // Deploy project, assets, and create stac items
      const deployed = await deployProject(proj, { ...args, source: args.source ?? rootCatalog }, q);
      collections.add(deployed);
    }

    if (collections.size === 0) {
      throw new Error(`Deploy: No QGIS projects deployed found in ${args.project.map((m) => String(m)).join(', ')}`);
    }

    logger.info({ project: args.project }, 'Deploy: Create Stac Catalog');

    await StacUpdater.collections(rootCatalog, [...collections.values()], true);

    logger.info({ project: args.project }, 'Deploy: Finished');
  },
});
