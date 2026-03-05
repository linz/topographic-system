import type { StacProvider } from 'stac-ts';

export const MediaTypes = {
  parquet: 'application/vnd.apache.parquet',
  geojson: 'application/geo+json',
  json: 'application/json',
  gpkg: 'application/geopackage+sqlite3',
  '': 'application/octet-stream',
};

export const Roles = {
  parquet: 'data',
  geojson: 'data',
  json: 'metadata',
  gpkg: 'data',
  '': 'data',
};

export const Providers: StacProvider[] = [
  { name: 'Land Information New Zealand', url: 'https://www.linz.govt.nz/', roles: ['processor', 'host'] },
];
