import { logger } from '@linzjs/topographic-system-shared';
import { command } from 'cmd-ts';

import { pyRunner } from '../python.runner.ts';

export const VersionCommand = command({
  name: 'version',
  description: 'Get qgis version',
  args: {},
  async handler() {
    const qgisVersion = await pyRunner.qgisVersion();
    logger.info({ qgisVersion }, 'Qgis:Version');
  },
});
