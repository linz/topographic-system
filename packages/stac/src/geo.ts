import type { Projection } from '@basemaps/geo';
import type { MultiPolygon } from '@linzjs/geojson';
import { union, Wgs84 } from '@linzjs/geojson';
import type { Position } from 'geojson';
import type { SpatialExtent, SpatialExtents, StacCatalog, StacCollection, StacItem } from 'stac-ts';
import type { GeoJSONMultiPolygon, GeoJSONPolygon } from 'stac-ts/src/types/geojson.js';

export function round(n: number, digits = 6): number {
  return Number(n.toFixed(digits));
}

export function polygonToWgs84(proj: Projection, polygon: Position[][]): Position[][] {
  return polygon.map((ring) =>
    ring.map((p) => {
      const [x, y] = proj.toWgs84(p);
      if (x == null || isNaN(x) || y == null || isNaN(y)) {
        throw new Error(`Invalid coordinate after projection transformation: [${x}, ${y}]`);
      }
      return [round(x), round(y)];
    }),
  );
}

export function multipolygonToWgs84(proj: Projection, multipolygon: Position[][][]): Position[][][] {
  return multipolygon.map((polygon) => polygonToWgs84(proj, polygon));
}

export type StacObject = StacItem | StacCollection | StacCatalog;
export const StacIs = {
  stac(x: unknown): x is StacObject {
    if (typeof x !== 'object') return false;
    if (x == null) return false;
    return 'stac_version' in x;
  },
  item(x: unknown): x is StacItem {
    return StacIs.stac(x) && x.type === 'Feature';
  },
  collection(x: unknown): x is StacCollection {
    return StacIs.stac(x) && x.type === 'Collection';
  },
  catalog(x: unknown): x is StacCatalog {
    return StacIs.stac(x) && x.type === 'Catalog';
  },
};

export const StacGeometry = {
  extend(base: StacItem | StacCollection, extension: StacItem | StacCollection) {
    if (StacIs.item(base)) return extendItem(base, extension);
    if (StacIs.collection(base)) return extendCollection(base, extension);
    throw new Error('extension is not a item or collection');
  },
};

function normalizePolygonToMultiPolygon(item: StacItem): number[][][][] {
  if (item.geometry?.type === 'Polygon') return [item.geometry.coordinates];
  if (item.geometry?.type === 'MultiPolygon') return item.geometry.coordinates;
  throw new Error('Unable to convert to polygon: ' + item.geometry?.type);
}

function bboxToMultiPolygon(bbox: number[] | [number, number, number, number]): number[][][][] {
  const sw = [bbox[0], bbox[1]] as [number, number];
  const se = [bbox[2], bbox[1]] as [number, number];
  const nw = [bbox[0], bbox[3]] as [number, number];
  const ne = [bbox[2], bbox[3]] as [number, number];

  if (bbox[0] < bbox[2]) return [[[sw, nw, ne, se, sw]]];

  return [[[sw, nw, [180, ne[1]], [180, se[1]], sw]], [[ne, se, [-180, sw[1]], [-180, nw[1]], ne]]];
}

function unionPolygon(polys: GeoJSONMultiPolygon['coordinates'][]): GeoJSONMultiPolygon['coordinates'] {
  return union(polys[0] as MultiPolygon, ...(polys.slice(1) as MultiPolygon[])) as GeoJSONMultiPolygon['coordinates'];
}

function toFeature(x: number[][][][]): GeoJSONPolygon | GeoJSONMultiPolygon {
  if (x.length === 1) return { type: 'Polygon', coordinates: x[0] as number[][][] };
  return { type: 'MultiPolygon', coordinates: x };
}

function extendItemFromCollection(base: StacItem, extension: StacCollection) {
  const bounds = [];
  if (base.geometry) bounds.push(normalizePolygonToMultiPolygon(base));
  for (const bbox of extension.extent.spatial.bbox) bounds.push(bboxToMultiPolygon(bbox));

  const target = unionPolygon(bounds);

  base.geometry = toFeature(target);
  base.bbox = Wgs84.multiPolygonToBbox(target as MultiPolygon);
}

function extendCollectionFromItem(base: StacCollection, extension: StacItem) {
  const bounds = [];
  if (extension.geometry) bounds.push(normalizePolygonToMultiPolygon(extension));
  for (const bbox of base.extent.spatial.bbox) bounds.push(bboxToMultiPolygon(bbox));
  const target = unionPolygon(bounds);

  base.extent.spatial.bbox = target.map(
    (m) => Wgs84.multiPolygonToBbox([m] as MultiPolygon) as SpatialExtent,
  ) as SpatialExtents;
}

// oxlint-disable no-unused-vars
function extendItemFromItem(_base: StacItem, _extension: StacItem) {
  throw new Error('Not yet implemented');
}

function extendCollectionFromCollection(_base: StacCollection, _extension: StacCollection) {
  throw new Error('Not yet implemented');
}

function extendItem(base: StacItem, extension: StacItem | StacCollection) {
  if (StacIs.item(extension)) return extendItemFromItem(base, extension);
  if (StacIs.collection(extension)) return extendItemFromCollection(base, extension);
  throw new Error('extension is not a item or collection');
}

function extendCollection(base: StacCollection, extension: StacItem | StacCollection) {
  if (StacIs.item(extension)) return extendCollectionFromItem(base, extension);
  if (StacIs.collection(extension)) return extendCollectionFromCollection(base, extension);
  throw new Error('extension is not a item or collection');
}
