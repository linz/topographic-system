import { createHash } from 'crypto';

import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import type { LimitFunction } from 'p-limit';
import type { StacAsset, StacCollection, StacItem, StacLink } from 'stac-ts';

import { StacBasic } from './stac.basic.ts';
import { getRelativePath } from './stac.paths.ts';
import type { StacStorageCategory, StorageStrategy } from './stac.storage.ts';
import { StacStorage } from './stac.storage.ts';

const StacSource = Symbol('stac.source');

export const HashWriter = {
  async write(asset: StacAsset | StacLink, target: URL, buffer: string | Buffer | URL) {
    if (buffer instanceof URL) return HashWriter.stream(asset, target, buffer);
    return HashWriter.file(asset, target, buffer)
  },
  async file(asset: StacAsset | StacLink, target: URL, buffer: string | Buffer): Promise<void> {
    const hash = createHash('sha256').update(buffer).digest('hex');
    await fsa.write(target, buffer, { contentType: asset.type });
    asset['file:checksum'] = `1220` + hash;
    asset['file:size'] = buffer.length;
  },
  async stream(asset: StacAsset | StacLink, target: URL, source: URL): Promise<void> {
    const ht = new HashTransform('sha256');
    const readStream = fsa.readStream(source).pipe(ht);
    await fsa.write(target, readStream, { contentType: asset.type });
    asset['file:checksum'] = ht.multihash;
    asset['file:size'] = ht.size;
  },
};

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

  itemAsset(itemName: string, assetName: string, source: URL|Buffer, asset: StacAsset) {
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

  async write(prefix: URL, q: LimitFunction, _commit: boolean = false): Promise<URL[]> {
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
        todo.push(q(() => HashWriter.write(asset, target, source)));
      }
    }

    await Promise.all(todo);

    for (const s of this.strategies) {
      const baseUrl = StacStorage.url(s, ctx);
      const targetCollection = structuredClone(this.collection);

      await Promise.all(
        [...this.items].map(([itemName, item]) => {
          return q(async () => {
            const itemUrl = new URL(`./${itemName}.json`, baseUrl);
            const targetItem = structuredClone(item);
            targetItem.links.unshift({ rel: 'self', href: `./${itemName}.json`, type: 'application/json' });
            targetItem.links.unshift({ rel: 'parent', href: `./collection.json`, type: 'application/json' });
            targetItem.links.unshift({ rel: 'root', href: '/catalog.json', type: 'application/json' });

            targetItem.id = StacStorage.id(s, ctx);

            for (const itemLink of targetItem.links) {
              itemLink.href = getRelativePath(new URL(itemLink.href, itemUrl), itemUrl);
            }

            const targetLink = targetCollection.links.find((f) => f.href === `./${itemName}.json`);
            if (targetLink == null) throw new Error(`item: ${itemName} is not found in collection`);

            await HashWriter.file(targetLink, itemUrl, JSON.stringify(targetItem, null, 2));
          });
        }),
      );

      const collectionUrl = new URL('collection.json', baseUrl);
      targetCollection.id = StacStorage.id(s, ctx);
      targetCollection.links.unshift({ rel: 'self', href: './collection.json', type: 'application/json' });
      targetCollection.links.unshift({ rel: 'root', href: '/catalog.json', type: 'application/json' });

      // Ensure latest links to canonical
      // and everything else links to latest
      if (s.type === 'latest') {
        if (strats.canonical != null) {
          const targetUrl = new URL("collection.json", StacStorage.url(strats.canonical, ctx));
          targetCollection.links.push({ rel: 'canonical', href: getRelativePath(targetUrl, collectionUrl) });
        }
      } else if (strats.latest != null) {
        const targetUrl = new URL("collection.json", StacStorage.url(strats.latest, ctx));
        targetCollection.links.push({ rel: 'latest-version', href: getRelativePath(targetUrl, collectionUrl) });
      }

      await fsa.write(collectionUrl, JSON.stringify(targetCollection, null, 2), { contentType: 'application/json' });
      collectionUrls.push(collectionUrl);
    }

    return collectionUrls;
  }
}
