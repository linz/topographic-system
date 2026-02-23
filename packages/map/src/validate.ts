import { fsa } from '@chunkd/fs';
import { Tiff } from '@cogeotiff/core';

export async function validateTiff(url: URL, epsg: number): Promise<void> {
  const tiff = await Tiff.create(fsa.source(url));
  const image = tiff.images[0];
  if (image == null) throw new Error('No images found in tiff');

  // Validate epsg matches
  if (image.epsg == null || image.epsg !== epsg) {
    throw new Error(`Invalid EPSG for ${url.href}: expected ${epsg}, got ${image.epsg}`);
  }

  await tiff.source.close?.();
}
