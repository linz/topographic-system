import { fsa } from '@chunkd/fs';
import { qMapAll } from '@linzjs/topographic-system-shared';
import type { LimitFunction } from 'p-limit';

import { readCatalog } from './stac.downloader.ts';
import { StacStorage } from './stac.storage.ts';
import type { StacStorageCategory, StorageStrategy } from './stac.storage.ts';

/**
 * Find collection.json paths filtered by a storage strategy from a STAC catalog.
 * Supports commit-based (`{ type: 'commit', commit: sha }`) and date-based (`{ type: 'date', date: new Date() }`) strategies.
 * Only returns layers that have data matching the strategy.
 * Throws an error if no layers are found.
 *
 * @param catalogUrl Catalog.json URL (e.g., /data/catalog.json, /qgis/catalog.json, etc.)
 * @param strategy Storage strategy to filter by — commit or date
 * @param q Concurrency queue to limit parallel HTTP requests
 * @returns Map of layer names to their collection.json URLs for that strategy
 * @throws Error if no layers have strategy-specific data
 */
export async function getCollectionsByStrategy(
  catalogUrl: URL,
  strategy: StorageStrategy,
  q: LimitFunction,
): Promise<Map<string, URL>> {
  const catalog = await readCatalog(catalogUrl);

  // Derive the root prefix and category from the catalog URL (e.g. /data/catalog.json → prefix=/, category='data')
  const prefix = new URL('../', catalogUrl);
  const category = new URL('./', catalogUrl).pathname.split('/').filter(Boolean).at(-1) as StacStorageCategory;

  const layers: { title: string; strategyUrl: URL }[] = [];

  for (const link of catalog.links) {
    if (link.rel !== 'child') continue;
    if (link.title == null) continue;
    if (!link.href.endsWith('/catalog.json')) continue;
    layers.push({
      title: link.title,
      strategyUrl: new URL('collection.json', StacStorage.url(strategy, { prefix, category, label: link.title })),
    });
  }

  const results = await qMapAll(q, layers, async ({ title, strategyUrl }) => {
    const exists = await fsa.exists(strategyUrl).catch(() => false);
    return exists ? { title, strategyUrl } : null;
  });

  const collections = new Map<string, URL>();
  for (const result of results) {
    if (result == null) continue;
    collections.set(result.title, result.strategyUrl);
  }

  if (collections.size === 0) {
    throw new Error(`No data found for strategy ${JSON.stringify(strategy)} in catalog: ${catalogUrl.href}`);
  }

  return collections;
}
