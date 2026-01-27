import { logger } from '@topographic-system/shared/src/log.ts';
import { $, ProcessOutput } from 'zx';

export function callKartInSanitizedEnv(args: string[]): Promise<ProcessOutput> {
  $.env['GIT_TERMINAL_PROMPT'] = '0';
  const clearedEnvVars: string[] = [];
  const deletedEnvVars: string[] = [];
  for (const [key, value] of Object.entries($.env)) {
    if (value === '') {
      clearedEnvVars.push(key);
      // explicitly setting to a space to avoid kart/git issues with empty looking (in Node) env vars containing non-UTF8 data (in kart)
      $.env[key] = ' ';
    }
    if (!hasValidUtf8(value ?? '')) {
      // Leaving this in as a safeguard, though it looks like Node sanitizes this before we get it
      deletedEnvVars.push(key);
      delete $.env[key];
    }
  }
  if (deletedEnvVars.length > 0 || clearedEnvVars.length > 0) {
    logger.warn({ deletedEnvVars, clearedEnvVars }, 'Modified env vars for kart call');
  }
  return $`kart ${args}`;
}

/**
 * Check if a string contains a valid (decodable) UTF-8 sequences.
 * @param str
 * @return boolean
 */
function hasValidUtf8(str: string): boolean {
  try {
    const encoded = new TextEncoder().encode(str);
    const decoded = new TextDecoder('utf-8', { fatal: true }).decode(encoded);
    return decoded === str;
  } catch {
    return false;
  }
}
