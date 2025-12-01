import { fsa } from '@chunkd/fs';
import cmdts from 'cmd-ts';
import { relative } from 'path';
import { fileURLToPath, pathToFileURL } from 'url';

/**
 * Convert a path to a relative path
 *
 * @param path the path to convert
 * @param base the path to be relative to default to current path
 */
export function toRelative(path: URL, base: URL = fsa.toUrl(process.cwd())): string {
  if (path.protocol !== 'file:' || base.protocol !== 'file:') throw new Error('Must be file: URL');
  return './' + relative(fileURLToPath(base), fileURLToPath(path));
}

/**
 * Parse an input parameter as a URL.
 *
 * If it looks like a file path, it will be converted using `pathToFileURL`.
 **/
export const Url: cmdts.Type<string, URL> = {
  from(str) {
    try {
      return Promise.resolve(new URL(str));
    } catch (e) {
      return Promise.resolve(pathToFileURL(str));
    }
  },
};

/**
 * Parse an input parameter as a URL which represents a folder.
 *
 * If it looks like a file path, it will be converted using `pathToFileURL`.
 * Any search parameters or hash will be removed, and a trailing slash added
 * to the path section if it's not present.
 **/
export const UrlFolder: cmdts.Type<string, URL> = {
  async from(str) {
    const url = await Url.from(str);
    url.search = '';
    url.hash = '';
    if (!url.pathname.endsWith('/')) url.pathname += '/';
    return url;
  },
};
