import { fsa } from '@chunkd/fs';
import type { LimitFunction } from 'p-limit';
import type { StacAsset, StacCollection, StacItem } from 'stac-ts';

import { StacGeometry } from './geo.ts';
import { HashWriter } from './hash.writer.ts';
import { StacBasic } from './stac.basic.ts';
import { getRelativePath } from './stac.paths.ts';
import type { StacStorageCategory, StorageStrategy } from './stac.storage.ts';
import { StacStorage } from './stac.storage.ts';
const StacSource = Symbol('stac.source');

function getSource(x: unknown): URL | Buffer | string | null {
  if (typeof x !== 'object' || x == null) return null;
  if (StacSource in x) return x[StacSource] as URL;
  return null;
}

export class StacCollectionWriter {
  collection: StacCollection;
  strategies: StorageStrategy[] = [];

  items = new Map<string, StacItem>();

  category: StacStorageCategory;
  label: string;

  constructor(category: StacStorageCategory, label: string) {
    this.category = category;
    this.label = label;
    this.collection = StacBasic.collection();
  }

  strategy(s: StorageStrategy) {
    this.strategies.push(s);
  }

  item(itemName: string): StacItem {
    let current = this.items.get(itemName);
    if (current == null) {
      current = StacBasic.item(this.collection.id);
      this.collection.links.push({
        rel: 'item',
        href: `./${itemName}.json`,
        type: 'application/json',
      });
      this.items.set(itemName, current);
    }
    return current;
  }

  itemAsset(itemName: string, assetName: string, source: URL | Buffer, asset: StacAsset) {
    const item = this.item(itemName);
    item.assets ??= {};
    if (item.assets[assetName]) throw new Error(`Overriding asset on ${itemName}.${assetName}`);
    item.assets[assetName] = asset;
    Object.defineProperty(asset, StacSource, { enumerable: false, value: source });
  }

  asset(assetName: string, source: URL, asset: StacAsset) {
    this.collection.assets ??= {};
    if (this.collection.assets[assetName]) throw new Error(`Overriding asset on collection.${assetName}`);
    this.collection.assets[assetName] = asset;
    Object.defineProperty(asset, StacSource, { enumerable: false, value: source });
  }

  // TODO: Once all the cli updated to use stac push, We will replace this with StacLoader.push for strageies write, and StacCollectionWriter.write for local write.
  async writeWithStrategy(prefix: URL, q: LimitFunction, commit: boolean = false): Promise<URL[]> {
    const items = [...this.items.values()];

    const ctx = { prefix, category: this.category, label: this.label };

    const collectionUrls: URL[] = [];

    const strats = {
      latest: this.strategies.find((f) => f.type === 'latest'),
      canonical: this.strategies.find((f) => f.type !== 'latest'),
    };

    const todo: Promise<unknown>[] = [];
    const assets = [
      ...Object.values(this.collection.assets ?? {}),
      ...items.map((m) => Object.values(m.assets ?? {})).flat(),
    ];
    for (const s of this.strategies) {
      const baseUrl = StacStorage.url(s, ctx);

      for (const asset of assets) {
        const target = new URL(asset.href, baseUrl);
        const source = getSource(asset);
        if (source == null) continue; // TODO should this throw
        if (commit) todo.push(q(() => HashWriter.writeStac(asset, target, source)));
      }
    }

    await Promise.all(todo);

    // TODO should we be assigning geometires here?
    for (const item of this.items.values()) StacGeometry.extend(this.collection, item);

    for (const s of this.strategies) {
      const baseUrl = StacStorage.url(s, ctx);
      const targetCollection = structuredClone(this.collection);
      targetCollection.id = StacStorage.id(s, ctx);

      await Promise.all(
        [...this.items].map(([itemName, item]) => {
          return q(async () => {
            const itemUrl = new URL(`./${itemName}.json`, baseUrl);
            const targetItem = structuredClone(item);
            targetItem.links.unshift({ rel: 'self', href: `./${itemName}.json`, type: 'application/json' });
            targetItem.links.unshift({ rel: 'collection', href: `./collection.json`, type: 'application/json' });
            targetItem.links.unshift({ rel: 'root', href: '/catalog.json', type: 'application/json' });

            targetItem.id = StacStorage.id(s, { ...ctx, item: itemName });
            targetItem.collection = targetCollection.id;

            for (const itemLink of targetItem.links) {
              itemLink.href = getRelativePath(new URL(itemLink.href, itemUrl), itemUrl);
            }

            const targetLink = targetCollection.links.find((f) => f.href === `./${itemName}.json`);
            if (targetLink == null) throw new Error(`item: ${itemName} is not found in collection`);

            if (commit) await HashWriter.writeStac(targetLink, itemUrl, JSON.stringify(targetItem, null, 2));
          });
        }),
      );

      const collectionUrl = new URL('collection.json', baseUrl);
      targetCollection.links.unshift({ rel: 'self', href: './collection.json', type: 'application/json' });
      targetCollection.links.unshift({ rel: 'parent', href: '../catalog.json', type: 'application/json' });
      targetCollection.links.unshift({ rel: 'root', href: '/catalog.json', type: 'application/json' });

      // Ensure "latest" links to "canonical"
      // and everything else links to "latest"
      if (s.type === 'latest') {
        if (strats.canonical != null) {
          const targetUrl = new URL('collection.json', StacStorage.url(strats.canonical, ctx));
          targetCollection.links.push({ rel: 'canonical', href: getRelativePath(targetUrl, collectionUrl) });
        }
      } else if (strats.latest != null) {
        const targetUrl = new URL('collection.json', StacStorage.url(strats.latest, ctx));
        targetCollection.links.push({ rel: 'latest-version', href: getRelativePath(targetUrl, collectionUrl) });
      }

      if (commit) {
        await fsa.write(collectionUrl, JSON.stringify(targetCollection, null, 2), { contentType: 'application/json' });
      }
      collectionUrls.push(collectionUrl);
    }

    return collectionUrls;
  }

  async write(target: URL, q: LimitFunction): Promise<URL> {
    const items = [...this.items.values()];
    const todo: Promise<unknown>[] = [];
    const assets = [
      ...Object.values(this.collection.assets ?? {}),
      ...items.map((m) => Object.values(m.assets ?? {})).flat(),
    ];

    const baseUrl = new URL(`${this.label}/`, target);
    for (const asset of assets) {
      const target = new URL(asset.href, baseUrl);
      const source = getSource(asset);
      if (source == null) continue; // TODO should this throw?
      todo.push(q(() => HashWriter.writeStac(asset, target, source)));
    }

    await Promise.all(todo);

    // TODO should we be assigning geometries here?
    for (const item of this.items.values()) StacGeometry.extend(this.collection, item);
    const targetCollection = structuredClone(this.collection);
    targetCollection.id = `${this.category}-${this.label}`;
    await Promise.all(
      [...this.items].map(([itemName, item]) => {
        return q(async () => {
          const itemUrl = new URL(`./${itemName}.json`, baseUrl);
          const targetItem = structuredClone(item);
          targetItem.links.unshift({ rel: 'self', href: `./${itemName}.json`, type: 'application/json' });
          targetItem.links.unshift({ rel: 'collection', href: `./collection.json`, type: 'application/json' });
          targetItem.links.unshift({ rel: 'root', href: '/catalog.json', type: 'application/json' });

          targetItem.id = itemName;
          targetItem.collection = targetCollection.id;

          for (const itemLink of targetItem.links) {
            if (itemLink.rel === 'dataset') continue; // dataset links for qgis stac item should be absolute urls so we should not relativise them
            itemLink.href = getRelativePath(new URL(itemLink.href, itemUrl), itemUrl);
          }

          const targetLink = targetCollection.links.find((f) => f.href === `./${itemName}.json`);
          if (targetLink == null) throw new Error(`item: ${itemName} is not found in collection`);

          await HashWriter.writeStac(targetLink, itemUrl, JSON.stringify(targetItem, null, 2));
        });
      }),
    );

    const collectionUrl = new URL('collection.json', baseUrl);
    targetCollection.links.unshift({ rel: 'self', href: './collection.json', type: 'application/json' });
    targetCollection.links.unshift({ rel: 'parent', href: '../catalog.json', type: 'application/json' });
    targetCollection.links.unshift({ rel: 'root', href: '/catalog.json', type: 'application/json' });

    await fsa.write(collectionUrl, JSON.stringify(targetCollection, null, 2), { contentType: 'application/json' });

    return collectionUrl;
  }
}
