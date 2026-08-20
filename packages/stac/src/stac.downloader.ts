import { mkdir, symlink } from 'node:fs/promises';
import { basename } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { WriteOptions } from '@chunkd/fs';
import { fsa, HashTransform } from '@chunkd/fs';
import { logger } from '@linzjs/topographic-system-shared';
import { type LimitFunction } from 'p-limit';
import type { StacAsset, StacCatalog, StacCollection, StacItem, StacLink } from 'stac-ts';
import * as tar from 'tar';

import { StacUrlResolverCanonical } from './resolvers/canonical.ts';
import type { StacUrlResolver } from './resolvers/resolver.ts';
import { StacUrlResolverStrategy } from './resolvers/strategy.ts';
import { StacLruCache } from './stac.lru.ts';

export interface SourceAsset {
  /** resolved source URL */
  source: URL;
  /** File location on disk */
  target: URL;
  /** Number of bytes in the file */
  size: number;
  /** multihash of the file if it exists */
  hash: string;
}

type CheckLink = (link: StacLink) => boolean;
type CheckAsset = (asset: StacAsset) => boolean;

export class StacDownloader {
  target: URL;
  cache: URL;
  q: LimitFunction;

  static Resolver = { strategy: (strat: string) => new StacUrlResolverStrategy(strat) };
  linkCache: Map<string, SourceAsset> = new Map();

  resolvers: StacUrlResolver[] = [new StacUrlResolverCanonical()];
  resolved = new Map<string, Promise<URL>>();
  resolutionStats = new Map<string, { invokes: 0, resolves: 0 }>();

  lru = new StacLruCache(1000);

  // Inflight downloads
  downloads: Map<string, Promise<SourceAsset>> = new Map();

  constructor(target: URL, cache: URL, q: LimitFunction) {
    this.target = target;
    this.cache = cache;
    this.q = q;
  }

  async resolveUrl(url: URL): Promise<URL> {
    let existing = this.resolved.get(url.href);
    if (existing != null) return existing;

    const resolver = this.q(async () => {
      for (const r of this.resolvers) {
        const stat = this.resolutionStats.get(r.name) ?? { invokes: 0, resolves: 0 };
        this.resolutionStats.set(r.name, stat);
        stat.invokes++;
        const afterUrl = await r.resolve(this.lru, url);
        if (afterUrl.href !== url.href) stat.resolves ++;
        url = afterUrl;
      }
      return url;
    });
    this.resolved.set(url.href, resolver);
    return resolver;
  }

  async fetchStac<T>(url: URL): Promise<{ url: URL; asset: T }> {
    const resolved = await this.resolveUrl(url);
    if (resolved == null) throw new Error('Unable to resolve: ' + url);
    const asset = (await this.lru.fetch(resolved)) as T;

    if (url.href !== resolved.href) {
      logger.debug({ url: url.href, resolved: resolved.href }, 'Url:Resolved');
    }
    return { url: resolved, asset };
  }

  async fetchAssets(url: URL, assetCheck?: (asset: StacAsset) => boolean): Promise<SourceAsset[]> {
    const resolved = await this.fetchStac<StacItem | StacCollection>(url);
    const assets = resolved.asset?.assets;
    if (assets == null) throw new Error('No assets found: ' + URL);

    const toDownload: Promise<SourceAsset>[] = [];
    for (const asset of Object.values(assets)) {
      if (assetCheck?.(asset) === false) continue;
      const assetUrl = new URL(asset.href, resolved.url);
      toDownload.push(this.downloadAsset(assetUrl, asset));
    }

    return await Promise.all(toDownload);
  }

  async fetchLinkedAssets(url: URL, linkCheck?: CheckLink, assetCheck?: CheckAsset): Promise<SourceAsset[]> {
    const resolved = await this.fetchStac<StacItem | StacCollection>(url);
    if (url.href !== resolved.url.href) {
      logger.debug({ url: url.href, resolved: resolved.url.href }, 'Url:Resolved');
    }
    const links = resolved.asset?.links;
    if (links == null) throw new Error('No links found: ' + URL);

    const toDownload: Promise<SourceAsset[]>[] = [];
    for (const link of links) {
      if (linkCheck?.(link) === false) continue;
      const linkUrl = new URL(link.href, resolved.url);
      toDownload.push(this.fetchAssets(linkUrl, assetCheck));
    }

    return (await Promise.all(toDownload)).flat();
  }

  /**
   * Fetch a asset and store it in a persistent local cache based off its file:checksum
   */
  protected async ensureAssetInCache(
    asset: StacLink | StacAsset,
    url: URL,
  ): Promise<{ url: URL; size: number; hash: string; hit?: boolean }> {
    const checksum = asset['file:checksum'] as string | undefined;
    const fileSize = asset['file:size'] as number | undefined;

    if (checksum == null) throw new Error(`Asset has no "file:checksum" ${url.href}`);

    const cacheKey = new URL(`${checksum}_${basename(asset.href)}`, this.cache);
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

  /** Ensure the linked path is a symlink to the target file, creating it if it doesn't exist or is incorrect */
  protected async ensureLinkedPath(sourceUrl: URL, linkedUrl: URL): Promise<URL> {
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
  private async downloadAsset(url: URL, asset: StacAsset | StacLink): Promise<SourceAsset> {
    let existingDownload = this.downloads.get(url.href);
    if (existingDownload) return existingDownload;

    const resolvedUrl = await this.resolveUrl(url);
    existingDownload = this.downloads.get(resolvedUrl.href);
    if (existingDownload) return existingDownload;

    const downloadPromise = this._downloadAsset(resolvedUrl, asset);
    this.downloads.set(url.href, downloadPromise);
    this.downloads.set(resolvedUrl.href, downloadPromise);

    return downloadPromise;
  }

  private async _downloadAsset(url: URL, asset: StacAsset | StacLink): Promise<SourceAsset> {
    const resolvedUrl = await this.resolveUrl(url);
    const startTime = performance.now();
    logger.debug({ project: url.href, downloaded: this.target.href, startTime }, 'DownloadFile:Start');
    const linkedPath = new URL(basename(url.pathname), this.target);

    const existing = this.linkCache.get(linkedPath.href);
    if (existing) {
      // Already linked and matches the hash
      if (existing.hash === asset['file:checksum']) return existing;
      throw new Error('Duplicate file download: ' + resolvedUrl.href);
    }

    const cacheStat = await this.q(() => this.ensureAssetInCache(asset, resolvedUrl));

    const sourceAsset: SourceAsset = {
      source: resolvedUrl,
      target: linkedPath,
      size: cacheStat.size,
      hash: cacheStat.hash,
    };

    if (cacheStat.url.pathname.endsWith('.tar') || cacheStat.url.pathname.endsWith('.tar.zst')) {
      const startExtractTime = performance.now();
      await mkdir(this.target, { recursive: true });
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
        duration: performance.now() - startTime,
      },
      'DownloadFile:Done',
    );

    this.linkCache.set(linkedPath.href, sourceAsset);

    return sourceAsset;
  }
}

const CatalogCache = new Map<string, Promise<StacCatalog>>();

export function readCatalog(url: URL): Promise<StacCatalog> {
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
