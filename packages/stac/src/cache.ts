export const CacheControl = {
  // Mutable stac documents and assets change frequently
  Mutable: 'public, max-age=30',
  // Immutable stac documents can sometimes change but it is very infrequent
  StacJson: 'public, max-age=300, stale-while-revalidate=86400',
  // Immutable assets should never change.
  Asset: 'public, max-age=31536000, immutable',
};

export function getCacheControl(target: URL): string {
  const isStac = target.pathname.endsWith('.json');
  const isMutable = target.pathname.includes('/latest/') || target.pathname.includes('/next/');

  if (isMutable) return CacheControl.Mutable;
  if (isStac) return CacheControl.StacJson;
  return CacheControl.Asset;
}
