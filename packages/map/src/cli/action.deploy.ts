import { basename } from 'path';

import { fsa } from '@chunkd/fs';
import { getDataFromCatalog, logger, registerFileSystem, Url, UrlFolder } from '@linzjs/topographic-system-shared';
import type { StorageStrategy } from '@linzjs/topographic-system-stac';
import { StacCollectionWriter, StacUpdater, StorageStrategyMulti } from '@linzjs/topographic-system-stac';
import { command, flag, multioption, option, optional, restPositionals } from 'cmd-ts';
import type { LimitFunction } from 'p-limit';
import tar from 'tar-stream';

import { qFromArgs } from '../limit.ts';
import { pyRunner } from '../python.runner.ts';

async function buildTarBuffer(projectFolder: URL): Promise<URL | null> {
  const tarPack = tar.pack();
  const chunks: Buffer[] = [];

  tarPack.on('data', (chunk) => chunks.push(Buffer.from(chunk)));

  const projectFiles = await fsa.toArray(fsa.list(projectFolder));
  if (projectFiles.length === 0) return null;

  let fileCount = 0;
  for (const file of projectFiles) {
    const filename = basename(file.href);
    if (!filename) throw new Error(`Deploy: Invalid file path ${file.href}`);
    if (filename.endsWith('.tar')) continue; // TODO
    if (filename.endsWith('.qgs')) continue;

    const data = await fsa.read(file);
    tarPack.entry({ name: filename, size: data.byteLength }, data);
    fileCount++;
  }

  if (fileCount === 0) return null;

  tarPack.finalize();

  await new Promise<void>((resolve, reject) => {
    tarPack.on('end', resolve);
    tarPack.on('error', reject);
  });

  await fsa.write(new URL('assets.tar', projectFolder), Buffer.concat(chunks));
  return new URL('assets.tar', projectFolder);
}

async function deployProject(
  project: URL,
  args: {
    source: URL;
    target: URL;
    dataTag?: string;
    commit: boolean;
    strategies: StorageStrategy[];
  },
  q: LimitFunction,
): Promise<URL[]> {
  const projectName = basename(project.href, '.qgs');
  const layers = await pyRunner.listSourceLayers(project);
  if (layers.length === 0) throw new Error(`No source layers found in project ${project.href}`);

  const datasetLinks = await Promise.all(
    layers.map((layer) => q(async () => await getDataFromCatalog(args.source, layer))),
  );
  const tarLocation = await buildTarBuffer(new URL('.', project));

  const sw = new StacCollectionWriter('qgis', projectName);
  // TODO get from QGIS project file ??
  sw.collection.title = `Topographic System QGIS ${projectName} Projects.`;
  sw.collection.description = `LINZ Topographic QGIS Project Series ${projectName}.`;

  for (const st of args.strategies) sw.strategy(st);

  const item = sw.item(projectName);

  for (const url of datasetLinks) {
    item.links.push({ rel: 'dataset', href: url.href, type: 'application/json' });
  }

  sw.itemAsset(projectName, 'project', project, {
    href: `./${projectName}.qgs`,
    type: 'application/vnd.qgis.qgs+xml',
    roles: ['data'],
  });

  if (tarLocation) {
    sw.itemAsset(projectName, 'assets', tarLocation, {
      href: `./${projectName}.tar`,
      type: 'application/x-tar',
      roles: ['data'],
    });
  }

  logger.info({ source: project.href, destination: args.target }, 'Deploy: Create Commit Stac Item');
  return await sw.write(args.target, q, args.commit);
}

export const DeployArgs = {
  project: restPositionals({
    type: Url,
    description: 'QGIS Project to deploy.',
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
  commit: flag({
    long: 'commit',
    description: 'Actually start the import',
    defaultValue: () => false,
    defaultValueIsSerializable: true,
  }),
  strategies: multioption({
    long: 'strategy',
    type: StorageStrategyMulti,
    description: 'Storage strategies to use, for example --strategy=latest',
  }),
};

export const DeployCommand = command({
  name: 'deploy',
  description: 'Deploy all the qgs project files and assets into target s3 location.',
  args: DeployArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project, commit: args.commit, strategies: args.strategies }, 'Deploy: Started');

    if (args.strategies.length === 0) throw new Error('--strategy is missing');
    const q = qFromArgs(args);

    const rootCatalog = new URL('catalog.json', args.target);
    args.source ??= rootCatalog;

    const collections = new Set<URL>();

    for (const proj of args.project) {
      if (!proj.href.endsWith('.qgs')) throw new Error(`${proj.href} needs to end with .qgs`);

      // Deploy project, assets, and create stac items
      const deployed = await deployProject(proj, args, q);
      for (const u of deployed) collections.add(u);
    }

    if (collections.size === 0) {
      throw new Error(`Deploy: No QGIS projects deployed found in ${args.project.map((m) => String(m)).join(', ')}`);
    }

    logger.info({ project: args.project }, 'Deploy: Create Stac Catalog');

    await StacUpdater.collections(rootCatalog, [...collections.values()], args.commit);

    logger.info({ project: args.project, commit: args.commit ? 'Uploaded' : 'Dry Run' }, 'Deploy: Finished');
  },
});
