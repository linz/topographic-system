import { type BinaryLike, createHash } from 'crypto';

import { fsa } from '@chunkd/fs';
import baseX from 'base-x';

const Base58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
export const base58 = baseX(Base58);

/** Hash something with sha256 then encode it as a base58 text string */
export function sha256base58(obj: BinaryLike): string {
  return base58.encode(createHash('sha256').update(obj).digest());
}

export async function recursiveFileSearch(sourcePath: URL, extension?: string): Promise<URL[]> {
  const stat = await fsa.head(sourcePath);
  if (stat && stat.isDirectory) {
    const filePaths = await fsa.toArray(fsa.list(sourcePath, { recursive: true }));
    if (extension) {
      return filePaths.filter((filePath) => filePath.href.endsWith(extension));
    }
    return filePaths;
  }
  if (stat) {
    if (extension === undefined || sourcePath.href.endsWith(extension)) {
      return [sourcePath];
    }
  }
  return [];
}
