import path from 'path/posix';

/**
 * Convert two absolute paths into a relative path if possible
 *
 * @param target target file to reach
 * @param base base location to start
 * @returns relative path if possible, absolute target otherwise
 */
export function getRelativePath(target: URL, base: URL): string {
  // If hosts or protocols differ, a relative path isn't possible/practical
  if (!matchProtocolHost(target, base)) return target.href;
  const basePath = base.pathname.endsWith('/') ? base.pathname : path.dirname(base.pathname) + '/';
  const relPath = path.relative(basePath, target.pathname);

  if (relPath.startsWith('.')) return relPath;
  return `./${relPath}`;
}

/**
 * Return true if protocol and host match
 * e.g. same s3 bucket, same memory host, same file host
 */
function matchProtocolHost(urlA: URL, urlB: URL): boolean {
  return urlA.protocol === urlB.protocol && urlA.host === urlB.host;
}
