import { logger } from '@linzjs/topographic-system-shared';
import { $ } from 'zx';

/**
 * Running python commands to join contour and landcover to produce ice contour
 */
export async function iceContour(contour: URL, landcover: URL, output: URL): Promise<void> {
  const res =
    await $`uv run --directory /packages/data-prep src/data_prep/ice_contour.py --contour ${contour.pathname} --landcover ${landcover.pathname} --output ${output.pathname}`;
  logger.debug('ice_contour.py ' + res.stdout);
}

/**
 * Running python commands to build the coastlines and islands polygon layer from
 * coastline lines and island polygons
 */
export async function coastlinePolygon(coastline: URL, island: URL, output: URL): Promise<void> {
  const res =
    await $`uv run --directory /packages/data-prep src/data_prep/coastline_polygon.py --coastline ${coastline.pathname} --island ${island.pathname} --output ${output.pathname}`;
  logger.debug('coastline_polygon.py ' + res.stdout);
}
