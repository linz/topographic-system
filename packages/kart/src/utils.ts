import { logger } from '@topographic-system/shared/src/log.ts';
import { $, ProcessOutput } from 'zx';

export function callKartInSanitizedEnv(args: string[]): Promise<ProcessOutput> {
  for (const [key, value] of Object.entries($.env)) {
    if (value && hasInvalidUtf8(value)) {
      logger.warn({ key, value: summarizeValue(value) }, 'Removing env var with invalid UTF-8');
      delete $.env[key];
    }
  }
  return $`kart ${args}`;
}

/**
 * Check if a string contains invalid UTF-8 sequences.
 * @param str
 * @return boolean
 */
function hasInvalidUtf8(str: string): boolean {
  // Check for lone surrogates (indicates invalid UTF-8 was decoded with errors)
  for (let i = 0; i < str.length; i++) {
    const code = str.charCodeAt(i);
    // Lone high surrogate (0xD800-0xDBFF) not followed by low surrogate
    // or lone low surrogate (0xDC00-0xDFFF)
    if (code >= 0xd800 && code <= 0xdfff) {
      if (code <= 0xdbff) {
        // High surrogate - check if followed by low surrogate
        const next = str.charCodeAt(i + 1);
        if (isNaN(next) || next < 0xdc00 || next > 0xdfff) {
          return true;
        }
        i++; // Skip the valid low surrogate
      } else {
        // Lone low surrogate
        return true;
      }
    }
  }
  // Also check for common corruption patterns like control chars in unexpected places
  // \x80-\xFF as single bytes would be invalid UTF-8 start bytes
  // In JS strings, these might appear as replacement chars or get mangled
  if (/[\x00-\x08\x0B\x0C\x0E-\x1F]/.test(str)) {
    // Suspicious control characters (excluding tab, newline, carriage return)
    return true;
  }
  return false;
}

/**
 * Summarize a string value for logging, escaping non-printable characters.
 * @param value
 * @return string
 */
function summarizeValue(value: string): string {
  const escaped = value
    .slice(0, 50)
    .replace(/[^\x20-\x7E]/g, (c) => `\\x${c.charCodeAt(0).toString(16).padStart(2, '0')}`);
  return value.length > 50 ? `${escaped}...` : escaped;
}
