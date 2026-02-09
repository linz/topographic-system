import { Tiff } from '@cogeotiff/core';
import { fsa } from '@chunkd/fs';
import { parse } from 'path';

import type { SheetMetadata } from './python.runner.ts';

export async function validateTiff(url: URL, metadata: SheetMetadata[]): Promise<void> {
  const sheetCode = parse(url.pathname).name;
  const sheetMeta = metadata.find((sheet) => sheet.sheetCode === sheetCode);
  if (sheetMeta == null) throw new Error(`No metadata found for sheet code: ${sheetCode}`);
  const tiff = await Tiff.create(fsa.source(url));
  const image = tiff.images[0];
  if (image == null) throw new Error('No images found in tiff');

  // Validate epsg matches
  if (image.epsg == null || image.epsg !== sheetMeta.epsg) {
    throw new Error(`Invalid EPSG for sheet ${sheetCode}: expected ${sheetMeta.epsg}, got ${image.epsg}`);
  }

  await tiff.source.close?.();
}
