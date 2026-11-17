/**
 * Current (published, not next/in-development) schema release -- see
 * packages/schema/src/features/common.tsp's `Versions` enum.
 */
export const SchemaVersion = 'v0.3';

export const SchemaHost = new URL('https://d1jzh93b1t1cv.cloudfront.net/schema/');

/** Public URL of a dataset's published JSON schema, as recorded on its kart dataset's `linz_schema` meta item. */
export function schemaUrl(datasetName: string, version: string = SchemaVersion): URL {
  return new URL(`${version}/${datasetName}.json`, SchemaHost);
}
