import { createHash } from 'crypto';

import type { WriteOptions } from '@chunkd/fs';
import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import type { StacAsset, StacLink } from 'stac-ts';
import { logger } from '@linzjs/topographic-system-shared';

export interface StacFileChecksum {
  'file:checksum': string;
  'file:size': number;
}

export const HashWriter = {
  async write(target: URL, source: URL | string | Buffer, obj: WriteOptions): Promise<StacFileChecksum> {
    if (source instanceof URL) return HashWriter.stream(target, source, obj);
    return HashWriter.file(target, source, obj);
  },

  async writeStac(asset: StacAsset | StacLink, target: URL, buffer: string | Buffer | URL) {
    const stats = await HashWriter.write(target, buffer, { contentType: asset.type });
    asset['file:checksum'] = stats['file:checksum'];
    asset['file:size'] = stats['file:size'];
  },

  stat(buffer: string | Buffer): StacFileChecksum {
    const hash = createHash('sha256').update(buffer).digest('hex');
    return {
      'file:checksum': `1220` + hash,
      'file:size': buffer.length,
    };
  },

  async file(target: URL, buffer: string | Buffer, obj: WriteOptions): Promise<StacFileChecksum> {
    const startTime = performance.now();

    const hash = createHash('sha256').update(buffer).digest('hex');
    await fsa.write(target, buffer, obj);

    const duration = performance.now() - startTime;
    logger.info({ target: target.href, size: buffer.length, hash:  duration }, 'HashWriter:Write');
    return {
      'file:checksum': `1220` + hash,
      'file:size': buffer.length,
    };
  },

  async stream(target: URL, source: URL, obj: WriteOptions): Promise<StacFileChecksum> {
    const startTime = performance.now();
    
    const ht = new HashTransform('sha256');
    const readStream = fsa.readStream(source).pipe(ht);
    await fsa.write(target, readStream, obj);

    const duration = performance.now() - startTime;
    logger.info({ target: target.href, size: ht.bytesRead, hash:  duration }, 'HashWriter:Write:Stream');
    return {
      'file:checksum': ht.multihash,
      'file:size': ht.bytesRead,
    };
  },
};
