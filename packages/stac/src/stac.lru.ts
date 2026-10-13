import { fsa } from '@chunkd/fs';
import type { StacItem, StacCatalog, StacCollection } from 'stac-ts';

type StacObject = StacItem | StacCatalog | StacCollection;
export class StacLruCache {
  mapA = new Map<string, Promise<StacObject>>();
  mapB = new Map<string, Promise<StacObject>>();

  maxSize: number;

  constructor(maxSize = 100) {
    this.maxSize = maxSize;
  }

  /** Attempt to fetch an object from the cache synchronously, returning undefined if not found */
  fetchSync(url: URL): Promise<StacObject> | undefined {
    return this.mapA.get(url.href) ?? this.mapB.get(url.href);
  }

  /** Attempt to fetch an object from the cache, returning null if not found */
  tryFetch<T extends StacObject>(url: URL): Promise<T | null> {
    return this.fetch<T>(url).catch(() => null);
  }

  fetch<T extends StacObject>(url: URL): Promise<T> {
    if (!url.href.endsWith('.json')) throw new Error('StacLRU is only for JSON objects');
    let existing = this.mapA.get(url.href);
    if (existing != null) return existing as Promise<T>;
    existing = this.mapB.get(url.href);
    if (existing) {
      this.mapB.delete(url.href);
      this.mapA.set(url.href, existing);
      return existing as Promise<T>;
    }

    existing = fsa.readJson(url);
    this.mapA.set(url.href, existing);
    if (this.mapA.size > this.maxSize) {
      this.mapB = this.mapA;
      this.mapA = new Map();
    }
    return existing as Promise<T>;
  }
}
