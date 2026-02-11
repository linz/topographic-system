import { fsa } from '@chunkd/fs';

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
