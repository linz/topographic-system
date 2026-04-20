import type { ReadResponse, WriteOptions } from '@chunkd/fs';
import { fsa, FsError } from '@chunkd/fs';
import { qMap } from '@linzjs/topographic-system-shared';
import type { LimitFunction } from 'p-limit';
import type { StacCatalog, StacCollection, StacItem } from 'stac-ts';

import { StacIs } from './geo.ts';
import type { StacFileChecksum } from './hash.writer.ts';
import { HashWriter } from './hash.writer.ts';
import { StacBasic } from './stac.basic.ts';
import { getRelativePath } from './stac.paths.ts';

interface StacReadWrite {
  retries: number;
}

interface StacItemWithHash extends StacFileChecksum {
  url: URL;
  item: StacItem;
}

function isFileStatsSame(x: StacFileChecksum, y: StacFileChecksum): boolean {
  if (x['file:checksum'] !== y['file:checksum']) return false;
  if (x['file:size'] !== y['file:size']) return false;
  return true;
}

export const StacUpdater = {
  /**
   * Update all collection links to the items with the file:checksum and file:size properties
   *
   * @param itemUrls Items to update
   * @param q
   * @returns List of collections that have been updated
   */
  async items(itemUrls: URL[], q: LimitFunction, commit: boolean): Promise<URL[]> {
    const collections = new Map<string, { url: URL; items: Map<string, StacItemWithHash> }>();

    // Load and hash all items
    await Promise.all(
      qMap(q, itemUrls, async (item) => {
        const itemRaw = await fsa.read(item);
        const itemStac = JSON.parse(itemRaw.toString()) as StacItem;
        if (itemStac == null) throw new Error(`Item not found at ${item.href}`);
        if (StacIs.item(itemStac) === false) throw new Error(`Invalid item at ${item.href}`);

        const collection = itemStac.links.find((l) => l.rel === 'collection');
        if (collection == null) throw new Error(`Item ${item.href} does not have a collection link`);
        const collectionUrl = new URL(collection.href, item);

        const mapping = collections.get(collectionUrl.href) ?? { url: collectionUrl, items: new Map() };
        const relativeItemUrl = getRelativePath(item, collectionUrl);
        if (mapping.items.has(relativeItemUrl)) {
          throw new Error(`Duplicate item ${relativeItemUrl} in collection ${collectionUrl.href}`);
        }

        mapping.items.set(relativeItemUrl, { url: item, item: itemStac, ...HashWriter.stat(itemRaw) });
        collections.set(collectionUrl.href, mapping);
      }),
    );

    const updatedCollections: URL[] = [];

    await Promise.all(
      qMap(q, Array.from(collections.values()), async (toUpdate) => {
        return StacUpdater.readWriteJson<StacCollection>(toUpdate.url, (collection) => {
          if (collection == null) throw new Error(`Collection not found at ${toUpdate.url.href}`);

          let hasChanges = false;
          for (const link of collection.links) {
            if (link.rel !== 'item') continue;
            const itemMapping = toUpdate.items.get(link.href);
            if (itemMapping == null) continue;

            toUpdate.items.delete(link.href);

            if (isFileStatsSame(itemMapping, link)) continue;
            link['file:checksum'] = itemMapping['file:checksum'];
            link['file:size'] = itemMapping['file:size'];
            hasChanges = true;
          }

          // TODO: should these be append to the items
          if (toUpdate.items.size > 0) {
            throw new Error(`Unprocessed items in collection ${toUpdate.url.href}`);
          }

          if (commit === false) return null;

          if (hasChanges) {
            updatedCollections.push(toUpdate.url);
            return collection;
          }
          return null;
        });
      }),
    );

    return updatedCollections;
  },

  /**
   * Given a list of collections, ensure they are linked in the parent catalogs up to the root, and attempt to write them
   *
   * @param root
   * @param collections
   * @param commit
   * @returns
   */
  async collections(root: URL, collections: URL[], commit: boolean): Promise<URL[]> {
    const targetCatalogs = new Map<string, { url: URL; children: Set<string> }>();

    for (const col of collections) {
      const rel = getRelativePath(root, col);
      if (!rel.startsWith('.')) throw new Error(`Collection:${col.href} is not relative to ${root.href}`);

      let nextCatalog = new URL('../catalog.json', col);
      let currentLink = col;

      function addMapping(next: URL, child: URL) {
        const mapping = targetCatalogs.get(next.href) ?? { url: next, children: new Set() };
        mapping.children.add(child.href);
        targetCatalogs.set(next.href, mapping);
      }

      while (nextCatalog.href !== root.href) {
        addMapping(nextCatalog, currentLink);
        currentLink = nextCatalog;
        nextCatalog = new URL('../catalog.json', nextCatalog);
      }
      addMapping(nextCatalog, currentLink);
    }

    const toWrite = [...targetCatalogs.values()];
    // Write the leaf nodes first
    toWrite.sort((a, b) => b.url.href.length - a.url.href.length);

    function updateLink(links: StacCatalog['links'], source: URL, child: URL): boolean {
      let updated = false;
      const relativeLink = getRelativePath(new URL(child), source);
      let link = links.find((f) => f.href === relativeLink);
      if (link == null) {
        updated = true;
        const cc = catalogContext(root, child);
        links.push({ rel: 'child', href: relativeLink, type: 'application/json', title: cc.title });
        // TODO: do we want file:checksum and file:size hashes here,
        // it makes things easier to see when changes happen, but will cause lots of write contention
      }

      return updated;
    }

    if (commit === false) return toWrite.map((m) => m.url);

    const updatedCatalogs: URL[] = [];
    for (const ctx of toWrite) {
      await StacUpdater.readWriteJson<StacCatalog>(ctx.url, (catalog) => {
        const targetCatalog = catalog ?? StacBasic.catalog();
        let hasChanges = false;

        if (targetCatalog.id === '') {
          const cc = catalogContext(root, ctx.url);
          targetCatalog.id = cc.id;
          targetCatalog.title = cc.title;
          targetCatalog.description = cc.description;
          hasChanges = true;
        }

        for (const url of ctx.children) {
          let isUpdated = updateLink(targetCatalog.links, ctx.url, new URL(url));
          hasChanges = hasChanges || isUpdated;
        }

        if (hasChanges === false) return null;
        targetCatalog['updated'] = new Date().toISOString();
        updatedCatalogs.push(ctx.url);
        return targetCatalog;
      });
    }

    return updatedCatalogs;
  },

  /**
   * Attempt to write a location, by doing a read/write swap
   *
   * will attempt to retry upto {@see StacReadWrite.retries}
   *
   * @param url
   * @param cb
   * @returns
   */
  async readWriteJson<T>(url: URL, cb: (f: T | null) => T | null, opts?: StacReadWrite): Promise<void> {
    return retryWrite(async () => {
      const source = await tryRead(url);

      const ret = cb(source == null ? null : JSON.parse(String(source)));
      if (ret == null) return;

      const flags: WriteOptions = { contentType: 'application/json' };
      if (StacIs.item(ret)) flags.contentType = 'application/geo+json';

      if (source == null) flags.ifNoneMatch = '*';
      else flags.ifMatch = source.$metadata?.eTag;

      await fsa.write(url, JSON.stringify(ret, null, 2), flags);
    }, opts);
  },
};

async function tryRead(u: URL): Promise<ReadResponse | null> {
  try {
    return await fsa.read(u);
  } catch (e) {
    if (FsError.is(e) && e.code === 404) return null;
    throw e;
  }
}

async function retryWrite<T>(cb: () => Promise<T>, opts?: StacReadWrite): Promise<T> {
  let lastError: FsError | null = null;
  const retries = opts?.retries ?? 3;
  for (let i = 0; i < retries; i++) {
    try {
      return await cb();
    } catch (e) {
      if (FsError.is(e) && e.code === 412) {
        lastError = e;
        continue;
      }
      throw e;
    }
  }
  if (lastError != null) throw lastError;
  // Should not be possible to get here unless retries is 0
  throw new Error('Unable to write');
}

function catalogContext(root: URL, catalogUrl: URL): { id: string; title: string; description: string } {
  const relativeLink = getRelativePath(catalogUrl, root);

  const idParts = relativeLink.slice(2).split('/').slice(0, -1);

  // Root Catalog, this should only happen in tests, as the root catalog should generally always exist
  if (idParts.length === 0) {
    return {
      id: 'linz-topographic',
      title: 'LINZ Topographic',
      description: 'Root catalog for LINZ Topographic Datasets, Product and System',
    };
  }

  // Top level category and dataset/product level
  if (idParts.length < 3) {
    return {
      id: idParts.join('_'),
      title: `${idParts.at(-1)}`,
      description: `${idParts.at(-1)!} catalog for LINZ Topographic`,
    };
  }

  // All other datasets
  return {
    id: idParts.join('_'),
    title: `${idParts.slice(1).join('-')}`,
    description: `${idParts.slice(1).join('-')}`,
  };
}
