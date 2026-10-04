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

export const StacStorageCategoryTypes = ['qgis', 'data', 'product'] as const;
export type StacStorageCategory = (typeof StacStorageCategoryTypes)[number];

export type StorageStrategy = StorageStrategyLatest | StorageStrategyCommit | StorageStrategyDate;

/**
 * Store a mutable copy of the assets `latest/` location
 *
 * If another strategy is present latest will have a canonical link to the next strategy
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

function parseStorageStrategyDate(value: string | undefined): Date {
  if (value == null || value === '') throw new Error('Invalid date');

  const directDate = new Date(value);
  if (!isNaN(directDate.getTime())) return directDate;

  const pathSafeDate = value.match(/^(\d{4}-\d{2}-\d{2}T\d{2})-(\d{2})-(\d{2}(?:\.\d+)?)Z$/);
  if (pathSafeDate != null) {
    const normalized = `${pathSafeDate[1]}:${pathSafeDate[2]}:${pathSafeDate[3]}Z`;
    const normalizedDate = new Date(normalized);
    if (!isNaN(normalizedDate.getTime())) return normalizedDate;
  }

  throw new Error('Invalid date');
}

export const StorageStrategyParsers: { [K in StorageStrategyName]: StorageStrategyParser<K> } = {
  commit(obj: string): StorageStrategyCommit {
    return { type: 'commit', commit: obj.split(StorageStrategySep)[1] ?? '' };
  },
  latest(): StorageStrategyLatest {
    return { type: 'latest' };
  },
  date(obj: string): StorageStrategyDate {
    const value = obj.split(StorageStrategySep)[1];
    return { type: 'date', date: parseStorageStrategyDate(value) };
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

/**
 * Attempt to parse the storage context from a "/latest/" URL
 * @param url
 * @returns
 */
export function storageStrategyFromLatest(url: URL): StorageContext | null {
  const latest = url.href.lastIndexOf('/latest/');
  if (latest === -1) return null;

  const parts = url.href.slice(0, latest).split('/');
  const label = parts.at(-1);
  const category = parts.at(-2) as StacStorageCategory;
  const prefix = new URL(parts.slice(0, parts.length - 2).join('/') + '/');
  if (label == null || category == null) return null;
  if (!StacStorageCategoryTypes.includes(category)) return null;
  return { label, category, prefix };
}

interface StorageContextWithItem extends StorageContext {
  /** Optional item name */
  item?: string;
}
const storeToId = (store: StorageContextWithItem): string => `${store.category}_${store.label}`;
const storeToSuffix = (store: StorageContextWithItem): string => {
  if (store.item) return `-${store.item}`;
  return '';
};
const StorageStrategyId: { [K in StorageStrategyName]: StorageStrategyIdGen<K> } = {
  latest(store: StorageContextWithItem): string {
    return storeToId(store) + '_latest' + storeToSuffix(store);
  },
  commit(store: StorageContextWithItem, s: StorageStrategyCommit): string {
    return storeToId(store) + `_${s.commit}` + storeToSuffix(store);
  },
  date: function (store: StorageContextWithItem, s: StorageStrategyDate): string {
    return storeToId(store) + `_${s.date.toISOString().replaceAll(':', '-')}` + storeToSuffix(store);
  },
};

export const StacStorage = {
  /** Generate a id for a item or collection   */
  id(s: StorageStrategy, ctx: StorageContext & { item?: string }) {
    return StorageStrategyId[s.type](ctx, s as any);
  },
  /** Generate a target folder URL for where the assets should be stored */
  url(s: StorageStrategy, ctx: StorageContext) {
    return StorageStrategyUrl[s.type](ctx, s as any);
  },
};
