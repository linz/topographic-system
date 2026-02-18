import { Command } from '@linzjs/docker-command';
import { logger, toRelative } from '@topographic-system/shared/src/index.ts';

/**
 * Running python commands to join contour and landcover
 */
export async function contourWithLandcover(contour: URL, landcover: URL, output: URL): Promise<void> {
  const cmd = Command.create('uv');
  cmd.args.push('run');
  cmd.args.push('--directory');
  cmd.args.push('/packages/data-prep');
  cmd.args.push('src/contour_with_landcover.py');
  cmd.args.push(toRelative(contour));
  cmd.args.push(toRelative(landcover));
  cmd.args.push(toRelative(output));
  const res = await cmd.run();
  logger.debug('contour_with_landcover.py ' + cmd.args.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ contour_with_landcover: res }, 'Failure');
    throw new Error('contour_with_landcover.py failed to run');
  }
}
