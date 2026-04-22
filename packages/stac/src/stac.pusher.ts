import { fsa } from '@chunkd/fs';
import type { LimitFunction } from 'p-limit';
import type { StacAsset, StacCatalog, StacCollection, StacItem } from 'stac-ts';

import { HashWriter } from './hash.writer.ts';
import { getRelativePath } from './stac.paths.ts';
import type { StorageContext, StacStorageCategory, StorageStrategy } from './stac.storage.ts';
import { StacStorage } from './stac.storage.ts';

export class StacPusher {
  catalogs = new Map<URL, StacCatalog>();
  collections = new Map<URL, StacCollection>();
  items = new Map<URL, StacItem>();
  assets = new Map<URL, URL>();

  category: StacStorageCategory;
  strategies: StorageStrategy[] = [];
  target: URL;
  targetCategory: URL;

  constructor(target: URL, category: StacStorageCategory) {
    this.target = target;
    this.targetCategory = new URL(`${category}/`, target);
    this.category = category;
  }

  private static strategyPriority(s: StorageStrategy): number {
    if (s.type === 'date') return 0;
    if (s.type === 'commit') return 1;
    return 2; // latest
  }

  strategy(s: StorageStrategy) {
    this.strategies.push(s);
    this.strategies.sort((a, b) => StacPusher.strategyPriority(a) - StacPusher.strategyPriority(b));
  }

  async loadCatalog(catalogUrl: URL) {
    const catalog = await fsa.readJson<StacCatalog>(catalogUrl);
    if (catalog == null) throw new Error(`Catalog not found or Invalid  at ${catalogUrl.href}`);
    this.catalogs.set(catalogUrl, catalog);
    for (const link of catalog.links) {
      if (link.rel !== 'child') continue;
      if (link.href.endsWith('collection.json')) {
        const collectionUrl = new URL(link.href, catalogUrl);
        await this.loadCollection(collectionUrl);
      }
      if (link.href.endsWith('catalog.json')) {
        const itemUrl = new URL(link.href, catalogUrl);
        await this.loadCatalog(itemUrl);
      }
    }
  }

  async loadCollection(collectionUrl: URL) {
    const collection = await fsa.readJson<StacCollection>(collectionUrl);
    if (collection == null) throw new Error(`Collection not found or Invalid at ${collectionUrl.href}`);
    this.collections.set(collectionUrl, collection);
    for (const link of collection.links) {
      if (link.rel !== 'item') continue;
      const itemUrl = new URL(link.href, collectionUrl);
      await this.loadItem(itemUrl);
    }
    for (const asset of Object.values(collection.assets ?? {})) {
      if (asset.href == null) continue;
      const assetUrl = new URL(asset.href, collectionUrl);
      this.assets.set(assetUrl, assetUrl);
    }
  }

  async loadItem(itemUrl: URL) {
    const item = await fsa.readJson<StacItem>(itemUrl);
    if (item == null) throw new Error(`Item not found or Invalid at ${itemUrl.href}`);
    this.items.set(itemUrl, item);
    for (const asset of Object.values(item.assets ?? {})) {
      if (asset.href == null) continue;
      const assetUrl = new URL(asset.href, itemUrl);
      this.assets.set(assetUrl, assetUrl);
    }
  }

  prepareStorageContext(prefix: URL, source: URL): { ctx: StorageContext; filename: string } {
    const splits = source.pathname.split('/').filter(Boolean);
    const label = splits[splits.length - 2];
    const filename = splits[splits.length - 1];
    if (label == null || filename == null || !filename.endsWith('.json')) {
      throw new Error(`Invalid source URL ${source.href}`);
    }
    const ctx: StorageContext = { prefix, category: this.category, label };
    return { ctx, filename };
  }

  async pushAsset(asset: StacAsset, url: URL, target: URL): Promise<URL> {
    const assetUrl = new URL(asset.href, url);
    const targetAssetUrl = new URL(asset.href, target);
    await HashWriter.writeStac(asset, targetAssetUrl, assetUrl);
    return targetAssetUrl;
  }

  async pushItems(
    url: URL,
    target: URL,
    collection: StacCollection,
    s: StorageStrategy,
    q: LimitFunction,
    commit: boolean,
  ): Promise<URL[]> {
    const todo: Promise<unknown>[] = [];
    const items: URL[] = [];
    for (const link of collection.links) {
      if (link.rel !== 'item') continue;
      const itemUrl = new URL(link.href, url);
      const item = await fsa.readJson<StacItem>(itemUrl);
      if (item == null) throw new Error(`Item not found or Invalid at ${itemUrl.href}`);
      const { ctx, filename } = this.prepareStorageContext(target, itemUrl);
      const itemName = filename.replace('.json', '');
      const targetUrl = StacStorage.url(s, ctx);
      const targetItemUrl = new URL(filename, targetUrl);
      item.id = StacStorage.id(s, { ...ctx, item: itemName });
      item.collection = collection.id;
      items.push(targetItemUrl);
      if (commit) {
        todo.push(q(() => HashWriter.writeStac(link, targetItemUrl, JSON.stringify(item, null, 2))));

        // Push assets
        for (const asset of Object.values(item.assets ?? {})) {
          if (asset.href == null) continue;
          todo.push(q(() => this.pushAsset(asset, url, targetUrl)));
        }
      }
    }
    await Promise.all(todo);
    return items;
  }

  async push(source: URL, q: LimitFunction, commit: boolean = false): Promise<{ items: URL[]; collections: URL[] }> {
    // Load all stac files for push
    await this.loadCatalog(source);

    const strats = {
      latest: this.strategies.find((f) => f.type === 'latest'),
      canonical: this.strategies.find((f) => f.type !== 'latest'),
    };
    const items: URL[] = [];
    const collections: URL[] = [];
    for (const s of this.strategies) {
      const todo: Promise<unknown>[] = [];
      for (const [url, collection] of this.collections) {
        const { ctx, filename } = this.prepareStorageContext(this.target, url);
        const targetUrl = StacStorage.url(s, ctx);
        const targetCollectionUrl = new URL(filename, targetUrl);
        const targetCollection = structuredClone(collection);
        targetCollection.id = StacStorage.id(s, ctx);

        // Push stac items and item assets
        items.push(...(await this.pushItems(url, this.target, targetCollection, s, q, commit)));

        // Prepare the canonical and latest links between collections
        if (s.type === 'latest') {
          if (strats.canonical != null) {
            const canonicalUrl = new URL('collection.json', StacStorage.url(strats.canonical, ctx));
            targetCollection.links.push({
              rel: 'canonical',
              href: getRelativePath(canonicalUrl, targetCollectionUrl),
            });
          }
        } else if (strats.latest != null) {
          const latestUrl = new URL('collection.json', StacStorage.url(strats.latest, ctx));
          targetCollection.links.push({
            rel: 'latest-version',
            href: getRelativePath(latestUrl, targetCollectionUrl),
          });
        }
        collections.push(targetCollectionUrl);
        if (commit) {
          todo.push(
            q(() =>
              HashWriter.write(targetCollectionUrl, JSON.stringify(targetCollection, null, 2), {
                contentType: 'application/json',
              }),
            ),
          );
          // Push collection assets
          for (const asset of Object.values(collection.assets ?? {})) {
            if (asset.href == null) continue;
            todo.push(q(() => this.pushAsset(asset, url, targetUrl)));
          }
        }
      }
      await Promise.all(todo);
    }
    return { items, collections };
  }
}
