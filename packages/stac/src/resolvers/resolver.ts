import type { StacLruCache } from '../stac.lru.ts';

export interface StacUrlResolver {
  name: string;
  resolve(fetcher: StacLruCache, url: URL): Promise<URL>;
}
