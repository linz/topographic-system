import { parseStrategy } from '../parser.ts';
import type { StacLruCache } from '../stac.lru.ts';
import type { StorageStrategy } from '../stac.storage.ts';
import { storageStrategyFromLatest, StacStorage } from '../stac.storage.ts';
import type { StacUrlResolver } from './resolver.ts';

export class StacUrlResolverStrategy implements StacUrlResolver {
  stats = { invokes: 0, resolves: 0 };
  st: StorageStrategy;
  sourceStrategy: string;
  constructor(strat: string) {
    this.st = parseStrategy(strat);
    this.sourceStrategy = strat;
  }

  get name() {
    return 'strategy:' + this.sourceStrategy;
  }

  async resolve(lru: StacLruCache, url: URL): Promise<URL> {
    const context = storageStrategyFromLatest(url);
    if (context == null) return url;

    const targetFile = url.pathname.slice(url.pathname.lastIndexOf('/') + 1);
    const target = new URL(targetFile, StacStorage.url(this.st, context));
    const asset = await lru.tryFetch(target);
    if (asset == null) return url;
    return target;
  }
}
