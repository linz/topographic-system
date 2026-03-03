import { logger } from '@linzjs/topographic-system-shared';
import { $ } from 'zx';

/**
 * Running python commands to join contour and landcover
 */
export async function contourWithLandcover(contour: URL, landcover: URL, output: URL): Promise<void> {
  const command = [
    'uv run',
    '--directory /packages/data-prep',
    'src/data_prep/contour_with_landcover.py',
    contour.toString(),
    landcover.toString(),
    output.toString(),
  ];

  const res = await $`${command.join(' ')}`;
  logger.debug('contour_with_landcover.py ' + command.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ contour_with_landcover: res }, 'Failure');
    throw new Error('contour_with_landcover.py failed to run');
  }
}
