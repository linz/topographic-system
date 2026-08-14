import { fsa } from '@chunkd/fs';
import { readCatalog } from '@linzjs/topographic-system-shared';

/**
 * Find collection.json paths for a specific commit from the STAC catalog.
 * Searches for paths matching pattern: `/{layer}/commit_prefix={first_char}/commit={SHA}/collection.json`
 * Only returns layers that have data for the given commit.
 * Throws an error if no layers are found for the commit.
 *
 * @param catalogUrl Catalog.json URL (e.g., /data/catalog.json, /qgis/catalog.json, etc.)
 * @param commitSha Git commit SHA to filter by (full SHA, e.g. 'a30006782f863ae93a19e1f78adffa30c89f6e8f')
 * @returns Map of layer names to their collection.json URLs for that commit
 * @throws Error if no layers have commit-specific data
 */
export async function getCollectionsByCommit(catalogUrl: URL, commitSha: string): Promise<Map<string, URL>> {
    const catalog = await readCatalog(catalogUrl);
    const collections = new Map<string, URL>();

    // For each layer (e.g., /airport/catalog.json, /coastline/catalog.json)
    for (const link of catalog.links) {
        const match = link.href.match(/\/([^/]+)\/catalog\.json$/);
        if (!match) continue;

        const layerName = match[1]!;
        const layerCatalogUrl = new URL(link.href, catalogUrl);
        const commitColUrl = new URL(
            `commit_prefix=${commitSha.charAt(0)}/commit=${commitSha}/collection.json`,
            layerCatalogUrl,
        );

        if (await fsa.head(commitColUrl).catch(() => null)) {
            collections.set(layerName, commitColUrl);
        }
    }

    if (collections.size === 0) {
        throw new Error(`No data found for commit ${commitSha} in catalog: ${catalogUrl.href}`);
    }

    return collections;
}
