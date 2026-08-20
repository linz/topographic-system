import type { StacLruCache } from '../stac.lru.ts';

export interface StacUrlResolver {
  name: string;
  stats: {
    /** Number of times the resolver was invoked */
    invokes: number;
    /** Number of times the resolver successfully resolved a URL */
    resolves: number;
  };
  resolve(fetcher: StacLruCache, url: URL): Promise<URL>;
}
