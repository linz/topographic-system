import { logger } from '@topographic-system/shared/src/log.ts';
import { command } from 'cmd-ts';
import { $ } from 'zx';

export const versionCommand = command({
  name: 'version',
  description: 'Get kart version',
  args: {},
  async handler() {
    const kvOut = await $`kart --version`;
    const kartVersion = kvOut.stdout.split('\n')[0]?.split(',')?.[0] ?? 'unknown';
    logger.info({ kartVersion }, 'Using kart executable');
  },
});
