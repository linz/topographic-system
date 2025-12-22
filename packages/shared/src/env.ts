import { z } from 'zod/v4-mini';

import { logger } from './log.ts';

export function parseEnv<T extends z.ZodMiniObject>(obj: T): z.output<T> {
  const env = obj.safeParse({ ...process.env });
  if (env.error) {
    logger.fatal(
      { env: env.error.issues.map((m) => `$${m.path.join('.')}: ${m.message}`).flat() },
      'environment:invalid',
    );
    process.exit(1);
  }
  return env.data;
}
