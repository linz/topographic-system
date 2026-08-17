import type { Type } from 'cmd-ts';
import { z } from 'zod/v4';

export const ExportFormats = {
  Pdf: 'pdf',
  Tiff: 'tiff',
  GeoTiff: 'geotiff',
  Png: 'png',
  Webp: 'webp',
} as const;

export type ExportFormat = (typeof ExportFormats)[keyof typeof ExportFormats];

const ExportParser = z.object({
  layout: z.string(),
  dpi: z.coerce.number(),
  format: z.enum(ExportFormats),
  // STAC asset label used in asset map
  label: z.optional(z.string()),
  // STAC asset "role"
  role: z.optional(z.string()),
});

export type ExportAsset = z.infer<typeof ExportParser>;

/**
 * Parse a format option string.
 *
 * Format is specified as a key-value specification (e.g. "layout=nztopo50,dpi=600,format=tiff" or "layout=nztopo50,dpi=30,format=webp,label=thumbnail")
 */
export function parseFormatOptionString(input: string): ExportAsset {
  const trimmed = input.trim();
  if (trimmed.length === 0) throw new Error('Empty format option string');

  const rawObj: Record<string, string> = {};
  for (const pt of trimmed.split(',')) {
    const eqIndex = pt.indexOf('=');
    if (eqIndex === -1) throw new Error(`Invalid key=value: "${pt}"`);
    const key = pt.slice(0, eqIndex);
    const value = pt.slice(eqIndex + 1);
    rawObj[key] = value;
  }

  const output = ExportParser.safeParse(rawObj);
  if (output.error) {
    throw new Error('Failed to parse format: ' + z.prettifyError(output.error));
  }
  // Default the label to thumbnail if the role is thumbnail
  if (output.data.role === 'thumbnail' && output.data.label == null) output.data.label = 'thumbnail';

  return output.data;
}

export const FormatMultiOption: Type<string[], ExportAsset[]> = {
  async from(strs) {
    return strs.flatMap((s) => parseFormatOptionString(s));
  },
};
