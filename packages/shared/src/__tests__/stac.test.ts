import assert from 'node:assert';
import { afterEach, before, beforeEach, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import type { StacCatalog } from 'stac-ts';

import { getDataFromCatalog } from '../stac.ts';

describe('stac-setup', () => {
  const mem = new FsMemory();
  const layer = 'water';
  const tag = 'latest';
  const rootCatalogLocation = fsa.toUrl('memory:///catalog.json');
  const dataCatalogLocation = fsa.toUrl(`memory:///${layer}/catalog.json`);
  const dataCollectionLocation = fsa.toUrl(`memory:///${layer}/${tag}/collection.json`);

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
        href: dataCollectionLocation.href,
        type: 'application/json',
      },
      {
        rel: 'child',
        href: fsa.toUrl(`memory:///${layer}/pr-11/collection.json`).href,
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
  });

  afterEach(() => {
    mem.files.clear();
  });

  it('should find collection by layer and tag', async () => {
    const collection = await getDataFromCatalog(rootCatalogLocation, layer, tag);
    assert.strictEqual(String(collection.href), String(dataCollectionLocation.href));
  });

  it('should throw an error with wrong layer name', async () => {
    await assert.rejects(async () => await getDataFromCatalog(rootCatalogLocation, 'waterrrr', tag), {
      message: `Layer waterrrr not found in catalog ${rootCatalogLocation.href}`,
    });
  });

  it('should throw an error with wrong tag', async () => {
    await assert.rejects(async () => await getDataFromCatalog(rootCatalogLocation, layer, 'taggggg'), {
      message: `Layer ${layer} with tag taggggg not found in catalog ${dataCatalogLocation.href}`,
    });
  });
});
