import type { ReadResponse, WriteOptions } from '@chunkd/fs';
import { fsa, FsError } from '@chunkd/fs';
import type { StacCatalog } from 'stac-ts';

import { StacIs } from './geo.ts';
import { StacBasic } from './stac.basic.ts';
import { getRelativePath } from './stac.paths.ts';

interface StacReadWrite {
  retries: number;
}

export const StacUpdater = {
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
    throw e
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
