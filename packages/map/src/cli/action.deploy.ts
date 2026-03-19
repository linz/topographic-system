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
import type { StacAsset, StacItem, StacLink } from 'stac-ts';
import tar from 'tar-stream';

import { pyRunner } from '../python.runner.ts';
import { createCatalog } from '../stac.ts';

type ProjectInfo = {
  file: URL;
  projectName: string;
  projectSeries: string;
  projectFolder: URL;
};

type ProjectPaths = {
  commitQgsPath: URL;
  derivedQgsPath: URL;
  commitTarPath: URL;
  derivedTarPath: URL;
  commitItemPath: URL;
  derivedItemPath: URL;
};

type DeployedProject = {
  series: string;
  projectName: string;
  item: StacItem;
};

async function buildTarBuffer(projectFolder: URL): Promise<Buffer | null> {
  const tarPack = tar.pack();
  const chunks: Buffer[] = [];

  tarPack.on('data', (chunk) => chunks.push(Buffer.from(chunk)));

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

  await new Promise<void>((resolve, reject) => {
    tarPack.on('end', resolve);
    tarPack.on('error', reject);
  });

  return Buffer.concat(chunks);
}

async function uploadFile(source: URL, target: URL, contentType: string): Promise<void> {
  logger.info({ source: source.href, destination: target.href }, 'Deploy: Upload File');
  const stream = fsa.readStream(source);
  await fsa.write(target, stream, { contentType });
}

async function uploadBuffer(buffer: Buffer, target: URL, contentType: string): Promise<void> {
  logger.info({ destination: target.href }, 'Deploy: Upload Buffer');
  await fsa.write(target, buffer, { contentType });
}

function getProjectInfo(projectRoot: URL, file: URL): ProjectInfo {
  const splits = file.href.split('/');
  const projectName = basename(file.href, '.qgs');
  const projectSeries = splits[splits.length - 2];

  if (projectName == null || projectSeries == null) {
    throw new Error(`Deploy: Invalid project file path ${file.href}`);
  }

  return {
    file,
    projectName,
    projectSeries,
    projectFolder: new URL(`${projectSeries}/`, projectRoot),
  };
}

function getProjectPaths(
  qgisCatalogPath: URL,
  projectSeries: string,
  projectName: string,
  commitTag: string,
  deployTag: string,
): ProjectPaths {
  return {
    commitQgsPath: new URL(`${projectSeries}/${commitTag}/${projectName}.qgs`, qgisCatalogPath),
    derivedQgsPath: new URL(`${projectSeries}/${deployTag}/${projectName}.qgs`, qgisCatalogPath),
    commitTarPath: new URL(`${projectSeries}/${commitTag}/${projectName}.tar`, qgisCatalogPath),
    derivedTarPath: new URL(`${projectSeries}/${deployTag}/${projectName}.tar`, qgisCatalogPath),
    commitItemPath: new URL(`${projectSeries}/${commitTag}/${projectName}.json`, qgisCatalogPath),
    derivedItemPath: new URL(`${projectSeries}/${deployTag}/${projectName}.json`, qgisCatalogPath),
  };
}

async function createDatasetLinks(source: URL, layers: string[], dataTag: string): Promise<StacLink[]> {
  const links = [];
  for (const layer of layers) {
    const layerCollection = await getDataFromCatalog(source, layer, dataTag);
    links.push({
      rel: 'dataset',
      href: layerCollection.href,
      type: 'application/json',
    });
  }
  return links;
}

function withAssetsLink(datasetLinks: StacLink[], tarPath: URL | null): StacLink[] {
  const links = [...datasetLinks];
  if (tarPath) {
    links.push({
      rel: 'assets',
      href: tarPath.href,
      type: 'application/x-tar',
    });
  }
  return links;
}

async function createProjectAsset(qgsPath: URL, file: URL): Promise<Record<string, StacAsset>> {
  return {
    project: {
      href: qgsPath.href,
      type: 'application/vnd.qgis.qgs+xml',
      roles: ['data'],
      ...(await createFileStats(file)),
    } as StacAsset,
  };
}

async function deployProject(
  project: ProjectInfo,
  paths: ProjectPaths,
  rootCatalogPath: URL,
  args: {
    source: URL;
    dataTag?: string;
    deployTag: string;
    commit: boolean;
  },
): Promise<DeployedProject> {
  const layers = await pyRunner.listSourceLayers(project.file);
  if (layers.length === 0) throw new Error(`No source layers found in project ${project.file.href}`);

  const datasetLinks = await createDatasetLinks(args.source, layers, args.dataTag ?? 'latest');
  const tarBuffer = await buildTarBuffer(project.projectFolder);

  if (args.commit) {
    await uploadFile(project.file, paths.commitQgsPath, 'application/vnd.qgis.qgs+xml');
    await uploadFile(project.file, paths.derivedQgsPath, 'application/vnd.qgis.qgs+xml');

    if (tarBuffer) {
      await uploadBuffer(tarBuffer, paths.commitTarPath, 'application/x-tar');
      await uploadBuffer(tarBuffer, paths.derivedTarPath, 'application/x-tar');
    }
  }

  const commitLinks = withAssetsLink(datasetLinks, tarBuffer ? paths.commitTarPath : null);
  const derivedLinks = withAssetsLink(datasetLinks, tarBuffer ? paths.derivedTarPath : null);

  const commitAssets = await createProjectAsset(paths.commitQgsPath, project.file);
  const derivedAssets = await createProjectAsset(paths.derivedQgsPath, project.file);

  logger.info({ source: project.file.href, destination: paths.commitItemPath.href }, 'Deploy: Create Commit Stac Item');
  const commitItem = createStacItem(rootCatalogPath, project.projectName, commitLinks, commitAssets);

  logger.info(
    { source: project.file.href, destination: paths.derivedItemPath.href },
    'Deploy: Create Derived Stac Item',
  );
  const derivedItem = {
    ...createStacItem(rootCatalogPath, project.projectName, derivedLinks, derivedAssets),
    id: `${args.deployTag}-${project.projectName}`,
    links: [
      ...derivedLinks,
      {
        rel: 'derived_from',
        href: paths.commitItemPath.href,
        type: 'application/json',
      },
    ],
  };

  if (args.commit) {
    logger.info({ destination: paths.commitItemPath.href }, 'Deploy: Upload Commit Stac Item');
    await fsa.write(paths.commitItemPath, JSON.stringify(commitItem, null, 2));

    logger.info({ destination: paths.derivedItemPath.href }, 'Deploy: Upload Derived Stac Item');
    await fsa.write(paths.derivedItemPath, JSON.stringify(derivedItem, null, 2));
  }

  return {
    series: project.projectSeries,
    projectName: project.projectName,
    item: commitItem,
  };
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

    const files = await fsa.toArray(fsa.list(args.project));
    const deployedProjects: DeployedProject[] = [];

    for (const file of files) {
      if (!file.href.endsWith('.qgs')) continue;

      const project = getProjectInfo(args.project, file);
      const paths = getProjectPaths(
        qgisCatalogPath,
        project.projectSeries,
        project.projectName,
        commitTag,
        args.deployTag,
      );

      // Deploy project, assets, and create stac items
      const deployed = await deployProject(project, paths, rootCatalogPath, args);
      deployedProjects.push(deployed);
    }

    if (deployedProjects.length === 0) {
      throw new Error(`Deploy: No QGS project files found in ${args.project.href}`);
    }

    // Group stac items by project series and create stac collections
    const qgisCatalogLinks: StacLink[] = [];
    const seriesMap = new Map<string, DeployedProject[]>();

    for (const deployed of deployedProjects) {
      const items = seriesMap.get(deployed.series) ?? [];
      items.push(deployed);
      seriesMap.set(deployed.series, items);
    }

    for (const [series, projects] of seriesMap) {
      logger.info({ mapSeries: series }, 'Deploy: Create Stac Collection');

      const title = `Topographic System QGIS ${series} Projects`;
      const description = `LINZ Topographic QGIS Project Series ${series}.`;

      const collectionLinks = projects.map((project) => ({
        rel: 'item',
        href: `./${project.projectName}.json`,
        type: 'application/json',
      }));

      const collection = createStacCollection(rootCatalogPath, description, [], collectionLinks);

      if (args.commit) {
        const stacCollectionPath = new URL(`${series}/${commitTag}/collection.json`, qgisCatalogPath);
        logger.info({ mapSeries: series, destination: stacCollectionPath.href }, 'Deploy: Upload Stac Collections');
        await fsa.write(stacCollectionPath, JSON.stringify(collection, null, 2));

        const derivedCollectionPath = new URL(`${series}/${args.deployTag}/collection.json`, qgisCatalogPath);
        const derivedCollection = {
          ...collection,
          id: `${args.deployTag}-${collection.id}`,
          links: [...collectionLinks, { rel: 'derived_from', href: stacCollectionPath.href, type: 'application/json' }],
        };

        logger.info(
          { mapSeries: series, destination: derivedCollectionPath.href },
          'Deploy: Upload Derived Stac Collections',
        );
        await fsa.write(derivedCollectionPath, JSON.stringify(derivedCollection, null, 2));

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

        const seriesCatalog = await createCatalog(seriesCatalogPath, rootCatalogPath, title, description, catalogLinks);

        logger.info({ destination: seriesCatalogPath.href }, 'Deploy: Upload Stac Catalog File');
        await fsa.write(seriesCatalogPath, JSON.stringify(seriesCatalog, null, 2));

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

      const link = {
        rel: 'child',
        href: `./qgis/catalog.json`,
        type: 'application/json',
      };

      const rootCatalog = await createCatalog(
        rootCatalogPath,
        rootCatalogPath,
        'LINZ Topographic System',
        'LINZ Topographic System Catalog',
        [link],
      );

      logger.info({ destination: rootCatalogPath.href }, 'Deploy: Update Root Catalog File');
      await fsa.write(rootCatalogPath, JSON.stringify(rootCatalog, null, 2));
    }

    logger.info({ project: args.project, commit: args.commit ? 'Uploaded' : 'Dry Run' }, 'Deploy: Finished');
  },
});
