import type { Projection } from '@basemaps/geo';
import type { Position } from 'geojson';

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
