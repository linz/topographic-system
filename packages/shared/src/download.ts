import { mkdir, symlink } from 'node:fs/promises';
import { basename } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { WriteOptions } from '@chunkd/fs';
import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import { logger, qMapAll } from '@linzjs/topographic-system-shared';
import { type LimitFunction } from 'p-limit';
import type { StacAsset, StacCatalog, StacCollection, StacItem, StacLink } from 'stac-ts';
import tar from 'tar';

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
  /** Previously written items */
  linkCache: Map<string, SourceAsset> = new Map();
  /** Local target location */
  target: URL;
  /** Cache asset files into the source cache */
  sourceCache: URL;

  constructor(target: URL, sourceCache: URL, q: LimitFunction) {
    this.q = q;
    this.target = target;
    this.sourceCache = sourceCache;
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
  async getAsset(url: URL, skipIfExists: boolean = false): Promise<SourceAsset[]> {
    const sourceStac = this.stacs.get(url.href);
    if (sourceStac == null) throw new Error(`Stac not added for url: ${url.href}`);
    if (sourceStac.assets.length > 0) return sourceStac.assets;

    const stac = await fsa.readJson<StacItem | StacCollection>(url);
    const sourceAssets = [];
    for (const [key, asset] of Object.entries(stac.assets ?? {})) {
      const sourceAsset = await this.downloadAsset(new URL(asset.href, url), asset, skipIfExists);
      sourceAssets.push(sourceAsset);
      if (key === 'project') {
        sourceStac.project = sourceAsset.linked;
      }
    }
    sourceStac.assets = sourceAssets;
    return sourceAssets;
  }

  /**
   * Fetch a asset and store it in a persistent local cache based off its file:checksum
   */
  async ensureAssetInCache(
    asset: StacLink | StacAsset,
    url: URL,
  ): Promise<{ url: URL; size: number; hash: string; hit?: boolean }> {
    const checksum = asset['file:checksum'] as string | undefined;
    const fileSize = asset['file:size'] as number | undefined;

    if (checksum == null) throw new Error(`Asset has no "file:checksum" ${url.href}`);

    const cacheKey = new URL(`${checksum}_${basename(asset.href)}`, this.sourceCache);
    const exists = await fsa.head(cacheKey).catch(() => null);

    if (exists) {
      if (exists.size !== fileSize) {
        logger.warn({ cacheKey: cacheKey.href, localSize: exists.size, expectedSize: fileSize }, 'Cache:Invalid');
        await fsa.delete(cacheKey);
      } else {
        return { url: cacheKey, size: exists.size as number, hash: asset['file:checksum'] as string, hit: true };
      }
    }

    const fileHash = new HashTransform('sha256');
    const stream = fsa.readStream(url).pipe(fileHash);
    const meta: WriteOptions = {};
    if (url.href.endsWith('.parquet')) meta.contentType = 'application/vnd.apache.parquet';
    await fsa.write(cacheKey, stream, meta);

    const head = await fsa.head(cacheKey);
    // validate file was downloaded correctly
    if (head?.size !== fileSize) {
      await fsa.delete(cacheKey);
      throw new Error(`Failed to download file: ${url.href} size mismatch ${head?.size} vs ${fileSize}`);
    }

    const targetHash = fileHash.multihash;
    if (targetHash !== checksum) {
      await fsa.delete(cacheKey);
      throw new Error(`Failed to download file: ${url.href} checksum mismatch ${targetHash}`);
    }

    return { url: cacheKey, size: head?.size as number, hash: targetHash };
  }

  /** Get all assets, downloading them if they haven't been already */
  async getAllAssets(skipIfExists: boolean = false): Promise<SourceAsset[]> {
    const allAssets = await qMapAll(this.q, Array.from(this.stacs.keys()), (url) =>
      this.getAsset(new URL(url), skipIfExists),
    );
    return allAssets.flat();
  }

  /** Ensure the linked path is a symlink to the target file, creating it if it doesn't exist or is incorrect */
  async ensureLinkedPath(sourceUrl: URL, linkedUrl: URL): Promise<URL> {
    // Symlinks are only supported on the local filesystem
    if (sourceUrl.protocol !== 'file:' || linkedUrl.protocol !== 'file:') {
      await fsa.write(linkedUrl, fsa.readStream(sourceUrl));
      return linkedUrl;
    }
    const [sourceExists, targetExists] = await Promise.all([fsa.exists(sourceUrl), fsa.exists(linkedUrl)]);
    if (!sourceExists) throw new Error(`Source file does not exist: ${sourceUrl.href}`);
    if (targetExists) {
      await fsa.delete(linkedUrl);
    } else {
      // ensure target folder exists
      await mkdir(this.target, { recursive: true });
    }
    await symlink(sourceUrl, linkedUrl);
    return linkedUrl;
  }

  /** Download given asset extract it if tar file */
  async downloadAsset(url: URL, asset: StacAsset | StacLink, skipIfExists: boolean): Promise<SourceAsset> {
    const startTime = performance.now();
    logger.debug({ project: url.href, downloaded: this.target.href, startTime }, 'DownloadFile:Start');
    const linkedPath = new URL(basename(url.pathname), this.target);

    const existing = this.linkCache.get(linkedPath.href);
    if (existing) {
      // Already linked and matches the hash
      if (existing.hash === asset['file:checksum']) return existing;
      if (skipIfExists) return existing;
    }

    const cacheStat = await this.ensureAssetInCache(asset, url);

    const sourceAsset: SourceAsset = {
      url,
      linked: linkedPath,
      size: cacheStat.size,
      hash: cacheStat.hash,
    };

    if (cacheStat.url.pathname.endsWith('.tar')) {
      const startExtractTime = performance.now();
      await tar.extract({
        file: fileURLToPath(cacheStat.url),
        cwd: fileURLToPath(this.target),
      });
      logger.info(
        {
          destination: cacheStat.url.href,
          ...sourceAsset,
          duration: performance.now() - startExtractTime,
        },
        'DownloadFile:Extract:Done',
      );
    } else {
      await this.ensureLinkedPath(cacheStat.url, linkedPath);
    }

    logger.info(
      {
        destination: cacheStat.url.href,
        ...sourceAsset,
        cacheHit: cacheStat.hit,
        overwrite: existing?.hash, // A file existed but was overwritten
        duration: performance.now() - startTime,
      },
      'DownloadFile:Done',
    );

    this.linkCache.set(linkedPath.href, sourceAsset);

    return sourceAsset;
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
