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

/** STAC Link "rel" that should be downloaded */
export const DownloadRels = new Set(['dataset', 'source', 'derived_from', 'project']);

function isRelative(href: string): boolean {
  try {
    new URL(href);
    return false;
  } catch {
    return true;
  }
}

export interface SourceFile {
  /** Source location */
  url: URL;
  /** Location to the downloaded file */
  asset?: URL;
  /** Linked Location to the downloaded file */
  linked?: URL;
  /** Number of bytes in the file */
  size?: number;
  /** multihash of the file if it exists */
  hash?: string;
}

export class Downloader {
  q: LimitFunction;
  /** Set of assets source location to download */
  assets: Map<string, SourceFile>;
  /** Local target location */
  target: URL;

  constructor(target: URL, q: LimitFunction) {
    this.q = q;
    this.assets = new Map<string, SourceFile>();
    this.target = target;
  }

  /** Add an asset URL to the download list */
  addAsset(url: URL): URL {
    if (!this.assets.has(url.href)) {
      logger.debug({ url: url.href }, 'Downloader: Add asset');
      this.assets.set(url.href, { url });
    } else {
      logger.debug({ url: url.href }, 'Downloader: Asset already added');
    }
    return url;
  }

  /** Add matching links from a STAC item/collection to the download list */
  addStacLinks(stac: StacItem | StacCollection, rels: Set<string>, baseUrl: URL): void {
    stac.links
      .filter((link) => rels.has(link.rel))
      .forEach((link) => {
        this.addAsset(isRelative(link.href) ? new URL(link.href, baseUrl) : new URL(link.href));
      });
  }

  /** Add all assets from a STAC item/collection to the download list */
  addStacAssets(stac: StacItem | StacCollection, baseUrl: URL): void {
    Object.values(stac.assets ?? {}).forEach((asset) => this.addAsset(new URL(asset.href, baseUrl)));
  }

  /** Get the linked path for the given asset URL, downloading it if it hasn't been already */
  async getAsset(url: URL): Promise<URL> {
    const asset = this.assets.get(url.href);
    if (asset == null) throw new Error(`Asset not added for url: ${url.href}`);
    if (asset.linked != null) return asset.linked;
    return await this.downloadAsset(asset);
  }

  /** Get all assets, downloading them if they haven't been already */
  async getAllAssets(): Promise<URL[]> {
    return await qMapAll(this.q, Array.from(this.assets.values()), (asset) => this.getAsset(asset.url));
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
  async downloadAsset(asset: SourceFile): Promise<URL> {
    const startTime = performance.now();
    logger.debug({ project: asset.url.href, downloaded: this.target.href, startTime }, 'DownloadFile:Start');
    try {
      const hashedFilename = sha256base58(Buffer.from(asset.url.href)) + extname(asset.url.href);
      const downloadFile = new URL(hashedFilename, this.target);
      const linkedPath = new URL(basename(asset.url.pathname), this.target);
      const stats = await fsa.head(asset.url);
      if (stats == null) throw new Error(`Unable to access file at url: ${asset.url.href}`);

      const fileHash = new HashTransform('sha256');
      const stream = fsa.readStream(asset.url).pipe(fileHash);
      const meta: WriteOptions = {};
      if (asset.url.href.endsWith('.parquet')) meta.contentType = 'application/vnd.apache.parquet';
      await fsa.write(downloadFile, stream, meta);

      const head = await fsa.head(downloadFile);
      // validate file was downloaded correctly
      if (head == null || head.size !== stats?.size) {
        throw new Error(`Failed to download file: ${downloadFile.href}`);
      }

      // Update asset with downloaded file info
      const digest = fileHash.multihash;
      asset.size = head.size;
      asset.hash = digest;
      asset.asset = downloadFile;
      asset.linked = linkedPath;

      logger.info(
        {
          ...asset,
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
            ...asset,
            duration: performance.now() - startExtractTime,
          },
          'DownloadFile:Extract:Done',
        );
      }
      return await this.ensureLinkedPath(downloadFile, linkedPath);
    } catch (error) {
      logger.error({ project: asset.url.href }, 'DownloadFile: Error');
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
