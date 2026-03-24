import type { Type } from 'cmd-ts';

import type { StorageStrategy, StorageStrategyName } from './stac.storage.ts';
import { StorageStrategySep, StorageStrategyParsers } from './stac.storage.ts';

export function parseStrategy(str: string): StorageStrategy {
  const key = str.split(StorageStrategySep)[0];
  const fn = StorageStrategyParsers[key as StorageStrategyName];
  if (fn == null) throw new Error('Invalid strategy');
  return fn(str);
}

export const StorageStrategyMulti: Type<string[], StorageStrategy[]> = {
  async from(str) {
    return str.map((m) => parseStrategy(m));
  },
};
