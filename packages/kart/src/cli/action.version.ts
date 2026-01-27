import { logger } from '@topographic-system/shared/src/log.ts';
import { command } from 'cmd-ts';

import { callKartInSanitizedEnv } from '../utils.ts';

export const versionCommand = command({
  name: 'version',
  description: 'Get kart version',
  args: {},
  async handler() {
    const kvOut = await callKartInSanitizedEnv(['--version']);
    const kartVersion = kvOut.stdout.split('\n')[0]?.split(',')?.[0] ?? 'unknown';
    logger.info({ kartVersion }, 'Using kart executable');
  },
});
