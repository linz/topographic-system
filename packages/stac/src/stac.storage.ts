/**
 * High level grouping of storage structure
 * 
 * These are the only valid top level keys in the target catalog
 * ```
 * - catalog.json
 * - data/catalog.json
 * - qgis/catalog.json
 * ```
 */
export type StacStorageCategory = 'qgis' | 'data';


export type StorageStrategy = StorageStrategyLatest | StorageStrategyCommit | StorageStrategyDate;

/**
 * Store a mutable copy of the assets `latest/` location
 * 
 * If another stategy is present latest will have a canonical link to the next strategy
 */
export type StorageStrategyLatest = { type: 'latest' };
/**
 * Stores a immutable copy of the assets inside commit_prefix={}/commit={} 
 * 
 * If "latest" is also preset will have a "latest-version" link to the latest folder
 */
export type StorageStrategyCommit = { type: 'commit'; commit: string };

/**
 * Stores a immutable copy of the assets inside year={}/date={} 
 * 
 * If "latest" is also preset will have a "latest-version" link to the latest folder
 */
export type StorageStrategyDate = { type: 'date'; date: Date };

export type StorageStrategyName = StorageStrategy['type'];

export interface StorageContext {
  prefix: URL;
  category: StacStorageCategory;
  label: string;
}

export type StorageStrategyParser<T extends StorageStrategyName> = (
  obj: string,
) => Extract<StorageStrategy, { type: T }>;
export type StorageStrategyPathGen<T extends StorageStrategyName> = (
  store: StorageContext,
  ctx: Extract<StorageStrategy, { type: T }>,
) => URL;
export type StorageStrategyIdGen<T extends StorageStrategyName> = (
  store: StorageContext,
  ctx: Extract<StorageStrategy, { type: T }>,
) => string;

export const StorageStrategySep = '=';

export const StorageStrategyParsers: { [K in StorageStrategyName]: StorageStrategyParser<K> } = {
  commit(obj: string): StorageStrategyCommit {
    return { type: 'commit', commit: obj.split(StorageStrategySep)[1] ?? '' };
  },
  latest(): StorageStrategyLatest {
    return { type: 'latest' };
  },
  date(obj: string): StorageStrategyDate {
    const value = obj.split(StorageStrategySep)[1];
    let date = value == null ? new Date() : new Date(value);
    if (isNaN(date.getTime())) throw new Error('Invalid date');
    return { type: 'date', date };
  },
};

const StorageStrategyUrl: { [K in StorageStrategyName]: StorageStrategyPathGen<K> } = {
  latest(store: StorageContext): URL {
    return new URL(`${store.category}/${store.label}/latest/`, store.prefix);
  },
  commit(store: StorageContext, s: StorageStrategyCommit): URL {
    return new URL(
      `${store.category}/${store.label}/commit_prefix=${s.commit.slice(0, 1)}/commit=${s.commit}/`,
      store.prefix,
    );
  },
  date: function (store: StorageContext, s: StorageStrategyDate): URL {
    return new URL(
      `${store.category}/${store.label}/year=${s.date.getUTCFullYear()}/date=${s.date.toISOString().replaceAll(':', '-')}/`,
      store.prefix,
    );
  },
};

const storeToId = (store: StorageContext): string => `${store.category}_${store.label}`;
const StorageStrategyId: { [K in StorageStrategyName]: StorageStrategyIdGen<K> } = {
  latest(store: StorageContext): string {
    return storeToId(store) + '_latest';
  },
  commit(store: StorageContext, s: StorageStrategyCommit): string {
    return storeToId(store) + `_${s.commit}`;
  },
  date: function (store: StorageContext, s: StorageStrategyDate): string {
    return storeToId(store) + `_${s.date.toISOString().replaceAll(':', '-')}`;
  },
};

export const StacStorage = {
  /** Generate a id for a item or collection   */
  id(s: StorageStrategy, ctx: StorageContext) {
    return StorageStrategyId[s.type](ctx, s as any);
  },
  /** Generate a target folder URL for where the assets should be stored */
  url(s: StorageStrategy, ctx: StorageContext) {
    return StorageStrategyUrl[s.type](ctx, s as any);
  },
};
