import assert from 'node:assert';
import { before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';

import { getCollectionsByCommit } from '../stac.catalog.ts';

describe('getCollectionsByCommit', () => {
    const mem = new FsMemory();

    before(() => {
        fsa.register('memory://', mem);
    });

    it('should find all commit-specific collections for all layers', async () => {
        const commitSha = 'a30006782f863ae93a19e1f78adffa30c89f6e8f';
        const catalogUrl = new URL('memory://data/catalog.json');

        const rootCatalog = {
            type: 'Catalog',
            id: 'data',
            description: 'Data catalog',
            links: [
                { rel: 'child', href: './airport/catalog.json' },
                { rel: 'child', href: './coastline/catalog.json' },
                { rel: 'child', href: './water/catalog.json' },
            ],
        };

        const airportCatalog = {
            type: 'Catalog',
            id: 'airport',
            description: 'Airport layer',
            links: [
                { rel: 'child', href: `./commit_prefix=a/commit=${commitSha}/collection.json` },
            ],
        };

        const coastlineCatalog = {
            type: 'Catalog',
            id: 'coastline',
            description: 'Coastline layer',
            links: [
                { rel: 'child', href: `./commit_prefix=a/commit=${commitSha}/collection.json` },
            ],
        };

        const waterCatalog = {
            type: 'Catalog',
            id: 'water',
            description: 'Water layer',
            links: [
                { rel: 'child', href: `./commit_prefix=a/commit=${commitSha}/collection.json` },
            ],
        };

        await fsa.write(catalogUrl, JSON.stringify(rootCatalog));
        await fsa.write(new URL('memory://data/airport/catalog.json'), JSON.stringify(airportCatalog));
        await fsa.write(new URL('memory://data/coastline/catalog.json'), JSON.stringify(coastlineCatalog));
        await fsa.write(new URL('memory://data/water/catalog.json'), JSON.stringify(waterCatalog));

        const collections = ['airport', 'coastline', 'water'];
        for (const layer of collections) {
            await fsa.write(
                new URL(`memory://data/${layer}/commit_prefix=a/commit=${commitSha}/collection.json`),
                JSON.stringify({ type: 'Collection', id: layer }),
            );
        }

        const result = await getCollectionsByCommit(catalogUrl, commitSha);

        assert.strictEqual(result.size, 3);
        assert.ok(result.has('airport'));
        assert.ok(result.has('coastline'));
        assert.ok(result.has('water'));
        assert.strictEqual(
            result.get('airport')?.href,
            'memory://data/airport/commit_prefix=a/commit=a30006782f863ae93a19e1f78adffa30c89f6e8f/collection.json',
        );
    });

    it('should only return layers that have commit-specific data', async () => {
        const commitSha = 'b40006782f863ae93a19e1f78adffa30c89f6e8f';
        const catalogUrl = new URL('memory://data-missing/catalog.json');

        const rootCatalog = {
            type: 'Catalog',
            id: 'data',
            description: 'Data catalog',
            links: [
                { rel: 'child', href: './airport/catalog.json' },
                { rel: 'child', href: './coastline/catalog.json' },
            ],
        };

        const airportCatalog = {
            type: 'Catalog',
            id: 'airport',
            description: 'Airport layer',
            links: [
                { rel: 'child', href: './commit_prefix=b/commit=b40006782f863ae93a19e1f78adffa30c89f6e8f/collection.json' },
            ],
        };

        const coastlineCatalog = {
            type: 'Catalog',
            id: 'coastline',
            description: 'Coastline layer',
            links: [
                { rel: 'child', href: './commit_prefix=b/commit=b40006782f863ae93a19e1f78adffa30c89f6e8f/collection.json' },
            ],
        };

        await fsa.write(catalogUrl, JSON.stringify(rootCatalog));
        await fsa.write(new URL('memory://data-missing/airport/catalog.json'), JSON.stringify(airportCatalog));
        await fsa.write(new URL('memory://data-missing/coastline/catalog.json'), JSON.stringify(coastlineCatalog));

        // Only airport has commit data, coastline does not
        await fsa.write(
            new URL(
                'memory://data-missing/airport/commit_prefix=b/commit=b40006782f863ae93a19e1f78adffa30c89f6e8f/collection.json',
            ),
            JSON.stringify({ type: 'Collection', id: 'airport' }),
        );

        const result = await getCollectionsByCommit(catalogUrl, commitSha);
        assert.strictEqual(result.size, 1);
        assert.ok(result.has('airport'));
        assert.ok(!result.has('coastline'));
    });

    it('should throw if no layers have commit-specific data', async () => {
        const commitSha = 'd99999782f863ae93a19e1f78adffa30c89f6e8f';
        const catalogUrl = new URL('memory://data-none/catalog.json');

        const rootCatalog = {
            type: 'Catalog',
            id: 'data',
            description: 'Data catalog',
            links: [{ rel: 'child', href: './airport/catalog.json' }],
        };

        const airportCatalog = {
            type: 'Catalog',
            id: 'airport',
            description: 'Airport layer',
            links: [],
        };

        await fsa.write(catalogUrl, JSON.stringify(rootCatalog));
        await fsa.write(new URL('memory://data-none/airport/catalog.json'), JSON.stringify(airportCatalog));

        await assert.rejects(getCollectionsByCommit(catalogUrl, commitSha), (err: Error) => {
            assert.ok(err.message.includes(`No data found for commit ${commitSha}`));
            return true;
        });
    });

    it('should handle different commit SHA prefixes correctly', async () => {
        const commitSha1 = 'a30006782f863ae93a19e1f78adffa30c89f6e8f';
        const commitSha2 = 'c50006782f863ae93a19e1f78adffa30c89f6e8f';
        const catalogUrl = new URL('memory://data-prefixes/catalog.json');

        const rootCatalog = {
            type: 'Catalog',
            id: 'data',
            description: 'Data catalog',
            links: [{ rel: 'child', href: './airport/catalog.json' }],
        };

        const airportCatalog = {
            type: 'Catalog',
            id: 'airport',
            description: 'Airport layer',
            links: [
                { rel: 'child', href: './commit_prefix=a/commit=a30006782f863ae93a19e1f78adffa30c89f6e8f/collection.json' },
                { rel: 'child', href: './commit_prefix=c/commit=c50006782f863ae93a19e1f78adffa30c89f6e8f/collection.json' },
            ],
        };

        await fsa.write(catalogUrl, JSON.stringify(rootCatalog));
        await fsa.write(new URL('memory://data-prefixes/airport/catalog.json'), JSON.stringify(airportCatalog));

        await fsa.write(
            new URL(`memory://data-prefixes/airport/commit_prefix=a/commit=${commitSha1}/collection.json`),
            JSON.stringify({ type: 'Collection', id: 'airport' }),
        );
        await fsa.write(
            new URL(`memory://data-prefixes/airport/commit_prefix=c/commit=${commitSha2}/collection.json`),
            JSON.stringify({ type: 'Collection', id: 'airport' }),
        );

        const result1 = await getCollectionsByCommit(catalogUrl, commitSha1);
        assert.strictEqual(result1.size, 1);
        assert.ok(result1.get('airport')?.href.includes('commit_prefix=a'));

        const result2 = await getCollectionsByCommit(catalogUrl, commitSha2);
        assert.strictEqual(result2.size, 1);
        assert.ok(result2.get('airport')?.href.includes('commit_prefix=c'));
    });

    it('should skip non-layer links in catalog', async () => {
        const commitSha = 'a30006782f863ae93a19e1f78adffa30c89f6e8f';
        const catalogUrl = new URL('memory://data-skip/catalog.json');

        const rootCatalog = {
            type: 'Catalog',
            id: 'data',
            description: 'Data catalog',
            links: [
                { rel: 'child', href: './parent' }, // doesn't end with /catalog.json
                { rel: 'child', href: './airport/catalog.json' },
                { rel: 'parent', href: '../root.json' }, // different rel type
            ],
        };

        const airportCatalog = {
            type: 'Catalog',
            id: 'airport',
            description: 'Airport layer',
            links: [
                { rel: 'child', href: './commit_prefix=a/commit=a30006782f863ae93a19e1f78adffa30c89f6e8f/collection.json' },
            ],
        };

        await fsa.write(catalogUrl, JSON.stringify(rootCatalog));
        await fsa.write(new URL('memory://data-skip/airport/catalog.json'), JSON.stringify(airportCatalog));
        await fsa.write(
            new URL(`memory://data-skip/airport/commit_prefix=a/commit=${commitSha}/collection.json`),
            JSON.stringify({ type: 'Collection', id: 'airport' }),
        );

        const result = await getCollectionsByCommit(catalogUrl, commitSha);
        assert.strictEqual(result.size, 1);
        assert.ok(result.has('airport'));
    });
});
