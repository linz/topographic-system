import { pino, destination } from 'pino';
import { PrettyTransform } from 'pretty-json-log';

import { CliId } from './cli.info.ts';

export const logger = process.stdout.isTTY
  ? pino(PrettyTransform.stream())
  : pino(destination({ dest: 1, sync: true }));
logger.level = 'debug';

logger.setBindings({ id: CliId });

export function registerLogger(cfg: { verbose?: boolean }): void {
  if (cfg.verbose) logger.level = 'trace';
}
