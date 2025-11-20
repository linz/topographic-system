import { pino } from 'pino';
import { PrettyTransform } from 'pretty-json-log';
import * as ulid from 'ulid';

export const logger = process.stdout.isTTY ? pino(PrettyTransform.stream()) : pino();
logger.level = 'debug';

export const logId = ulid.ulid();
logger.setBindings({ id: logId });

export function registerLogger(cfg: { verbose?: boolean }): void {
  if (cfg.verbose) logger.level = 'trace';
}