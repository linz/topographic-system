export const CacheControl = {
  StacJson: 'public, max-age=300, stale-while-revalidate=86400',
  Asset: 'public, max-age=31536000, immutable',
  AssetMutable: 'public, max-age=300, stale-while-revalidate=86400',
};

export function getAssetCacheControl(target: URL): string {
  if (target.pathname.endsWith('.json')) return CacheControl.StacJson;
  if (target.pathname.includes('/latest/')) return CacheControl.AssetMutable;
  if (target.pathname.includes('/next/')) return CacheControl.AssetMutable;
  return CacheControl.Asset;
}
