import { logger } from '@linzjs/topographic-system-shared';
import { $ } from 'zx';

/**
 * Running python commands to join contour and landcover
 */
export async function contourWithLandcover(contour: URL, landcover: URL, output: URL): Promise<void> {
  const res =
    await $`uv run --directory /packages/data-prep src/data_prep/contour_with_landcover.py --contour ${contour.pathname} --landcover ${landcover.pathname} --output ${output.pathname}`;
  logger.debug('contour_with_landcover.py ' + res.stdout);
}
