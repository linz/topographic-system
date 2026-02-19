import assert from 'node:assert';
import { afterEach, before, beforeEach, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import type { StacCatalog } from 'stac-ts';

import { getDataFromCatalog } from '../stac.upsert.ts';

describe('stac-setup', () => {
  const mem = new FsMemory();
  const layer = 'water';
  const tag = 'year=2026/date=2026-02-12T03:15:05.850Z';
  const rootCatalogLocation = fsa.toUrl('memory:///catalog.json');
  const dataCatalogLocation = fsa.toUrl(`memory:///${layer}/catalog.json`);
  const dataYearCatalogLocation = fsa.toUrl(`memory:///${layer}/year=2026/catalog.json`);
  const dataTagCollectionLocation = fsa.toUrl(`memory:///${layer}/${tag}/collection.json`);
  const latestCollectionLocation = fsa.toUrl(`memory:///${layer}/latest/collection.json`);

  const rootCatalog = {
    links: [
      { rel: 'self', href: rootCatalogLocation.href, type: 'application/json' },
      {
        rel: 'child',
        href: dataCatalogLocation.href,
        type: 'application/json',
      },
      {
        rel: 'child',
        href: fsa.toUrl(`memory:///water_line/catalog.json`).href,
        type: 'application/json',
      },
    ],
  } as StacCatalog;

  const dataCatalog = {
    links: [
      { rel: 'self', href: dataCatalogLocation.href, type: 'application/json' },
      {
        rel: 'child',
        href: latestCollectionLocation.href,
        type: 'application/json',
      },
      {
        rel: 'child',
        href: dataYearCatalogLocation.href,
        type: 'application/json',
      },
      {
        rel: 'child',
        href: fsa.toUrl(`memory:///${layer}/pull_request/catalog.json`).href,
        type: 'application/json',
      },
    ],
  };

  const dataYearCatalog = {
    links: [
      { rel: 'self', href: dataYearCatalogLocation.href, type: 'application/json' },
      {
        rel: 'child',
        href: latestCollectionLocation.href,
        type: 'application/json',
      },
      {
        rel: 'child',
        href: dataTagCollectionLocation.href,
        type: 'application/json',
      },
      {
        rel: 'child',
        href: fsa.toUrl(`memory:///${layer}/year=2026/date=2026-02-01T00:00:00.000Z/collection.json`).href,
        type: 'application/json',
      },
    ],
  };

  before(() => {
    fsa.register('memory:///', mem); // use 3 slashes to ensure URL is correct
  });

  beforeEach(async () => {
    await fsa.write(rootCatalogLocation, JSON.stringify(rootCatalog));
    await fsa.write(dataCatalogLocation, JSON.stringify(dataCatalog));
    await fsa.write(dataYearCatalogLocation, JSON.stringify(dataYearCatalog));
  });

  afterEach(() => {
    mem.files.clear();
  });

  it('should find collection by layer and tag', async () => {
    const collection = await getDataFromCatalog(rootCatalogLocation, layer, tag);
    assert.strictEqual(String(collection.href), String(dataTagCollectionLocation.href));
  });

  it('should find latest collection', async () => {
    const collection = await getDataFromCatalog(rootCatalogLocation, layer);
    assert.strictEqual(String(collection.href), String(latestCollectionLocation.href));
  });

  it('should throw an error with wrong layer name', async () => {
    await assert.rejects(async () => await getDataFromCatalog(rootCatalogLocation, 'waterrrr', tag), {
      message: `Layer waterrrr with tag ${tag} not found in catalog ${rootCatalogLocation.href}`,
    });
  });

  it('should throw an error with wrong tag', async () => {
    await assert.rejects(async () => await getDataFromCatalog(rootCatalogLocation, layer, 'taggggg'), {
      message: `Layer ${layer} with tag taggggg not found in catalog ${dataCatalogLocation.href}`,
    });
  });
});
