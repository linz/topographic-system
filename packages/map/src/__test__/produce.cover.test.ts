import assert from 'node:assert';
import { afterEach, before, beforeEach, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import type { StacCatalog } from 'stac-ts';

import { overrideSource, parseDataTag, sheetCodeToPath } from '../cli/action.produce.cover.ts';

describe('parseDataTag', () => {
  it('should parse correct data tags', () => {
    const result = parseDataTag(
      'airport/pull_request/pr-18/,contours/year=2026/date=2026-02-19T03:45:39.698Z/,landcover/latest/',
    );
    assert.deepEqual(result, [
      {
        layer: 'airport',
        tag: 'pull_request/pr-18',
      },
      {
        layer: 'contours',
        tag: 'year=2026/date=2026-02-19T03:45:39.698Z',
      },
      {
        layer: 'landcover',
        tag: 'latest',
      },
    ]);
  });
  it('Should parse one data tag', () => {
    const result = parseDataTag('airport/pull_request/pr-18');
    assert.deepEqual(result, [
      {
        layer: 'airport',
        tag: 'pull_request/pr-18',
      },
    ]);
  });
  it('Should not parse the invalid data tags', () => {
    assert.throws(() => parseDataTag('airport/pull_request/'), {
      message: `Invalid data tag format, expected "layer/latest", "layer/pull_request/pr-<number>", or "layer/year/<date>", got airport/pull_request/`,
    });
    assert.throws(() => parseDataTag('airport/pull_request/year=2026/date=2026-02-19T03:45:39.698Z'), {
      message: `Invalid data tag format, expected "layer/latest", "layer/pull_request/pr-<number>", or "layer/year/<date>", got airport/pull_request/year=2026/date=2026-02-19T03:45:39.698Z`,
    });
  });
});

describe('sheetCodeToPath', () => {
  it('Should convert sheet code to path', () => {
    assert.equal(sheetCodeToPath('AS21/AS22'), 'AS21AS22');
    assert.equal(sheetCodeToPath('CC19ptCC18'), 'CC19ptCC18');
    assert.equal(sheetCodeToPath('BJ43ptsBJ42,BH42,BH43'), 'BJ43ptsBJ42BH42BH43');
  });
});

describe('overrideSource', () => {
  const mem = new FsMemory();
  const layer = 'airport';
  const tag = 'pull_request/pr-18';
  const rootCatalogLocation = fsa.toUrl('memory:///catalog.json');
  const dataCatalogLocation = fsa.toUrl(`memory:///${layer}/catalog.json`);
  const pullRequestCatalogLocation = fsa.toUrl(`memory:///${layer}/pull_request/catalog.json`);
  const dataTagCollectionLocation = fsa.toUrl(`memory:///${layer}/${tag}/collection.json`);

  const rootCatalog = {
    links: [
      { rel: 'self', href: rootCatalogLocation.href, type: 'application/json' },
      {
        rel: 'child',
        href: dataCatalogLocation.href,
        type: 'application/json',
      },
    ],
  } as StacCatalog;

  const dataCatalog = {
    links: [
      { rel: 'self', href: dataCatalogLocation.href, type: 'application/json' },
      {
        rel: 'child',
        href: pullRequestCatalogLocation.href,
        type: 'application/json',
      },
    ],
  };

  const pullRequestCatalog = {
    links: [
      { rel: 'self', href: pullRequestCatalogLocation.href, type: 'application/json' },
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
    await fsa.write(pullRequestCatalogLocation, JSON.stringify(pullRequestCatalog));
  });

  afterEach(() => {
    mem.files.clear();
  });

  it('Should override the airport data by the tags', async () => {
    const airportTag = 'airport/pull_request/pr-18';
    const tags = parseDataTag(airportTag);
    const sources = [
      fsa.toUrl(`memory:///${layer}/latest/collection.json`),
      fsa.toUrl(`memory:///water/latest/collection.json`),
    ];
    await overrideSource(sources, tags, rootCatalogLocation);
    assert.strictEqual(sources.length, 2);
    assert.strictEqual(sources[0]!.href, `memory:///${airportTag}/collection.json`);
    assert.strictEqual(sources[1]!.href, `memory:///water/latest/collection.json`);
  });

  it('Should not orverride for tags not exists', async () => {
    const airportTag = 'airport/pull_request/pr-100';
    const tags = parseDataTag(airportTag);
    const sources = [
      fsa.toUrl(`memory:///${layer}/latest/collection.json`),
      fsa.toUrl(`memory:///water/latest/collection.json`),
    ];

    await assert.rejects(async () => await overrideSource(sources, tags, rootCatalogLocation), {
      message: `Layer airport with tag pull_request/pr-100 not found in catalog ${pullRequestCatalogLocation.href}`,
    });
  });
});
