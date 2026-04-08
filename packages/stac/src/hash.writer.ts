import { createHash } from 'crypto';

import type { WriteOptions } from '@chunkd/fs';
import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import type { StacAsset, StacLink } from 'stac-ts';

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
    const hash = createHash('sha256').update(buffer).digest('hex');
    await fsa.write(target, buffer, obj);

    return {
      'file:checksum': `1220` + hash,
      'file:size': buffer.length,
    };
  },

  async stream(target: URL, source: URL, obj: WriteOptions): Promise<StacFileChecksum> {
    const ht = new HashTransform('sha256');
    const readStream = fsa.readStream(source).pipe(ht);
    // FIXME: This is a hack to get the size of the stream into the write options. This should not be needed.
    //        Without `readStream.size = stat.size;`, writing large objects to S3 fails with a 500 error.
    //        E.g. "Expected 1 part(s) but uploaded 3 part(s)."
    const stat = await fsa.head(source);
    if (stat == null || stat.size == null) {
      throw new Error(`Unable to stat source ${source.href} for streaming write`);
    }
    readStream.size = stat.size;
    await fsa.write(target, readStream, obj);
    return {
      'file:checksum': ht.multihash,
      'file:size': stat.size, // FIXME: set back to ht.size when the above hack is removed
    };
  },
};
