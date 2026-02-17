import { Command } from '@linzjs/docker-command';
import { logger, toRelative, Url } from '@topographic-system/shared/src/index.ts';
import { command, option } from 'cmd-ts';

export const IceContoursArgs = {
  contour: option({
    type: Url,
    long: 'contour',
    description: 'Path or s3 of contour parquet',
  }),
  landcover: option({
    type: Url,
    long: 'landcover',
    description: 'Path or s3 of landcover parquet',
  }),
  output: option({
    type: Url,
    long: 'output',
    description: 'Path or s3 of output parquet',
  }),
};

export const IceContoursCommand = command({
  name: 'ice contours',
  description: 'Ice contours',
  args: IceContoursArgs,
  async handler(args) {
    logger.info({ args }, 'Prepare ice contours: Started');

    const cmd = Command.create('uv');
    cmd.args.push('run');
    cmd.args.push('--directory');
    cmd.args.push('/packages/data-prep');
    cmd.args.push('src/ice_contours.py');
    cmd.args.push(toRelative(args.contour));
    cmd.args.push(toRelative(args.landcover));
    cmd.args.push(toRelative(args.output));
    const res = await cmd.run();
    logger.debug('ice_contours.py ' + cmd.args.join(' '));

    if (res.exitCode !== 0) {
      logger.fatal({ ice_contours: res }, 'Failure');
      throw new Error('ice_contours.py failed to run');
    }
  },
});
