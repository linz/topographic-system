export function getAssetCacheControl(target: URL): string {
  const isStac = target.pathname.endsWith('.json');
  const isMutable = target.pathname.includes('/latest/') || target.pathname.includes('/next/');

  if (isStac) {
    if (isMutable) return 'public, max-age=30';
    // Immutable stac documents can sometimes change but it is very infrequent
    return 'public, max-age=300, stale-while-revalidate=86400'
  }

  if (isMutable) return 'public, max-age=30'
  // Immutable assets should never change.
  return 'public, max-age=31536000, immutable'
}
