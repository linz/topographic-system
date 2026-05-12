import { lstat, readlink, symlink, unlink } from 'node:fs/promises';
import { basename, dirname, extname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { WriteOptions } from '@chunkd/fs';
import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import { qMapAll } from '@linzjs/topographic-system-shared';
import { logger } from '@linzjs/topographic-system-shared';
import { type LimitFunction } from 'p-limit';
import type { StacCatalog, StacCollection, StacItem } from 'stac-ts';
import tar from 'tar';

import { sha256base58 } from './fs.util.ts';

export interface SourceAsset {
  /** downloaded URL */
  url: URL;
  /** Linked URL */
  linked: URL;
  /** Number of bytes in the file */
  size: number;
  /** multihash of the file if it exists */
  hash: string;
}

export interface SourceStac {
  /** Source stac file location */
  url: URL;

  /** stac assets to download */
  assets: SourceAsset[];

  /** Optional project download link if available */
  project?: URL;
}

/** STAC Link "rel" that should be downloaded */
export const DownloadRels = new Set(['dataset', 'source', 'derived_from', 'project']);

export class Downloader {
  q: LimitFunction;
  /** Cache of stac links that have been resolved to avoid duplicate downloads */
  stacs: Map<string, SourceStac> = new Map();
  /** Local target location */
  target: URL;
  /** Skip if linked path already exists */
  skip: boolean;

  constructor(target: URL, q: LimitFunction, skip = false) {
    this.q = q;
    this.stacs = new Map<string, SourceStac>();
    this.target = target;
    this.skip = skip;
  }

  /** Add an asset URL to the download list */
  addStac(url: URL): URL {
    if (!this.stacs.has(url.href)) {
      logger.debug({ url: url.href }, 'Downloader: Add asset');
      this.stacs.set(url.href, { url, assets: [] });
    } else {
      logger.debug({ url: url.href }, 'Downloader: Asset already added');
    }
    return url;
  }

  /** Add matching links from a STAC item/collection to the download list */
  addStacLinks(stac: StacItem | StacCollection, rels: Set<string>, baseUrl: URL): URL[] {
    const links = stac.links.filter((link) => rels.has(link.rel));
    return links.map((link) => this.addStac(new URL(link.href, baseUrl)));
  }

  /** Get the linked path for the given asset URL, downloading it if it hasn't been already */
  async getAsset(url: URL): Promise<SourceAsset[]> {
    const sourceStac = this.stacs.get(url.href);
    if (sourceStac == null) throw new Error(`Stac not added for url: ${url.href}`);
    if (sourceStac.assets.length > 0) return sourceStac.assets;

    const stac = await fsa.readJson<StacItem | StacCollection>(url);
    const sourceAssets = [];
    for (const [key, asset] of Object.entries(stac.assets ?? {})) {
      const sourceAsset = await this.downloadAsset(new URL(asset.href, url));
      sourceAssets.push(sourceAsset);
      if (key === 'project') {
        sourceStac.project = sourceAsset.linked;
      }
    }
    sourceStac.assets = sourceAssets;
    return sourceAssets;
  }

  /** Get all assets, downloading them if they haven't been already */
  async getAllAssets(): Promise<SourceAsset[]> {
    const allAssets = await qMapAll(this.q, Array.from(this.stacs.keys()), (url) => this.getAsset(new URL(url)));
    return allAssets.flat();
  }

  /** Ensure the linked path is a symlink to the target file, creating it if it doesn't exist or is incorrect */
  async ensureLinkedPath(targetFile: URL, linkedPath: URL): Promise<URL> {
    // Symlinks are only supported on the local filesystem
    if (targetFile.protocol !== 'file:' || linkedPath.protocol !== 'file:') return targetFile;
    const targetFsPath = fileURLToPath(targetFile);
    const linkedFsPath = fileURLToPath(linkedPath);
    const nextLinkTarget = relative(dirname(linkedFsPath), targetFsPath);

    try {
      const existing = await lstat(linkedFsPath);
      if (existing.isSymbolicLink()) {
        const currentLinkTarget = await readlink(linkedFsPath);
        const currentResolved = resolve(dirname(linkedFsPath), currentLinkTarget);
        if (currentResolved === targetFsPath) return linkedPath;
      }
      await unlink(linkedFsPath);
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }

    await symlink(nextLinkTarget, linkedFsPath);
    return linkedPath;
  }

  /** Download given asset extract it if tar file */
  async downloadAsset(url: URL): Promise<SourceAsset> {
    const startTime = performance.now();
    logger.debug({ project: url.href, downloaded: this.target.href, startTime }, 'DownloadFile:Start');
    try {
      const hashedFilename = sha256base58(Buffer.from(url.href)) + extname(url.href);
      const downloadFile = new URL(hashedFilename, this.target);
      const linkedPath = new URL(basename(url.pathname), this.target);
      if (this.skip) {
        if ((await fsa.head(linkedPath)) != null) {
          logger.info({ project: url.href }, 'DownloadFile: Skip download, linked file already exists');
          return { url, linked: linkedPath, size: 0, hash: '' };
        }
      }
      const stats = await fsa.head(url);
      if (stats == null) throw new Error(`Unable to access file at url: ${url.href}`);

      const fileHash = new HashTransform('sha256');
      const stream = fsa.readStream(url).pipe(fileHash);
      const meta: WriteOptions = {};
      if (url.href.endsWith('.parquet')) meta.contentType = 'application/vnd.apache.parquet';
      await fsa.write(downloadFile, stream, meta);

      const head = await fsa.head(downloadFile);
      // validate file was downloaded correctly
      if (head == null || head.size == null || head.size !== stats?.size) {
        throw new Error(`Failed to download file: ${downloadFile.href}`);
      }

      // Update asset with downloaded file info
      const digest = fileHash.multihash;
      const sourceAsset: SourceAsset = {
        url,
        linked: linkedPath,
        size: head.size,
        hash: digest,
      };

      logger.info(
        {
          destination: downloadFile.href,
          ...sourceAsset,
          duration: performance.now() - startTime,
        },
        'DownloadFile:Done',
      );

      // Extract tar files if needed
      if (downloadFile.pathname.endsWith('.tar')) {
        const startExtractTime = performance.now();
        await tar.extract({
          file: fileURLToPath(downloadFile),
          cwd: fileURLToPath(this.target),
        });
        logger.info(
          {
            destination: downloadFile.href,
            ...sourceAsset,
            duration: performance.now() - startExtractTime,
          },
          'DownloadFile:Extract:Done',
        );
      }
      await this.ensureLinkedPath(downloadFile, linkedPath);
      return sourceAsset;
    } catch (error) {
      logger.error({ project: url.href }, 'DownloadFile: Error');
      throw error;
    }
  }
}

const CatalogCache = new Map<string, Promise<StacCatalog>>();

function readCatalog(url: URL): Promise<StacCatalog> {
  let existing = CatalogCache.get(url.href);
  if (existing) return existing;
  existing = fsa.readJson<StacCatalog>(url);
  CatalogCache.set(url.href, existing);
  return existing;
}

/**
 * Recursively find the target data collection.json from the root catalog,
 *
 * @param stacUrl The URL of the root STAC catalog.
 * @param layerName The name of the vector layer to find.
 *
 * @returns Target data collection.json URL if found, otherwise throws an error.
 */
export async function getDataFromCatalog(stacUrl: URL, layerName: string): Promise<URL> {
  const catalog = await readCatalog(stacUrl);

  const targetLayer = `/${layerName}/catalog.json`;
  const catLink = catalog.links.find((link) => link.href.endsWith(targetLayer));
  if (catLink) {
    const catUrl = new URL(catLink.href, stacUrl); // /data/airport/catalog.json
    return new URL('latest/collection.json', catUrl); // /data/airport/latest/collection.json
  }

  const dataLink = catalog.links.find((link) => link.href.endsWith('/data/catalog.json'));
  if (dataLink) return getDataFromCatalog(new URL(dataLink.href, stacUrl), layerName);

  throw new Error(`Layer ${layerName} not found in catalog ${stacUrl.href}`);
}
