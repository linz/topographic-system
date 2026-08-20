import type { StacItem, StacCollection } from 'stac-ts';

import type { StacLruCache } from '../stac.lru.ts';
import type { StacUrlResolver } from './resolver.ts';

export class StacUrlResolverCanonical implements StacUrlResolver {
  name = 'canonical';

  async resolve(lru: StacLruCache, url: URL, visited: Set<string> = new Set()): Promise<URL> {
    if (visited.has(url.href)) throw new Error(`Circular canonical link detected: ${url.href}`);
    visited.add(url.href);

    if (!url.pathname.endsWith('.json')) return url;

    const asset = await lru.tryFetch<StacItem | StacCollection>(url);
    if (asset == null) throw new Error('Unable to fetch asset for canonical resolution: ' + url.href);
    if (asset?.links == null) return url;
    const canonical = asset.links.find((link) => link.rel === 'canonical');
    if (canonical == null) return url;

    const nextUrl = new URL(canonical.href, url);
    // Sometimes canonical links to it self.
    if (nextUrl.href === url.href) return url;
    if (visited.has(nextUrl.href)) throw new Error(`Circular canonical link detected: ${nextUrl.href}`);

    return this.resolve(lru, nextUrl, visited);
  }
}
