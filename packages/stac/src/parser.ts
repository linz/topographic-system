import type { Type } from 'cmd-ts';

import type { StorageStrategy, StorageStrategyName } from './stac.storage.ts';
import { StorageStrategyParsers, StorageStrategySep } from './stac.storage.ts';

// You can add more strategies in one parameter with a StorageStrategyKeySep.
const StorageStrategyKeySep = ',';

export function parseStrategy(str: string): StorageStrategy[] {
  const splits = str.split(StorageStrategyKeySep);
  return splits.map((s) => {
    const key = s.split(StorageStrategySep)[0];
    const fn = StorageStrategyParsers[key as StorageStrategyName];
    if (fn == null) throw new Error('Invalid strategy');
    return fn(s);
  });
}

export const StorageStrategyMulti: Type<string[], StorageStrategy[]> = {
  async from(str) {
    return str.flatMap((m) => parseStrategy(m));
  },
};
