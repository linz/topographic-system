import path from 'path/posix'

/**
 * Convert two absolute paths into a relative path if possible
 * 
 * @param target target file to reach
 * @param base base location to start
 * @returns relative path if possible, absolute target otherwise
 */
export function getRelativePath(target: URL, base: URL): string {
  // If hosts or protocols differ, a relative path isn't possible/practical
  if (target.origin !== base.origin) return target.href
  const basePath = base.pathname.endsWith('/') ? base.pathname : path.dirname(base.pathname) + '/'
  const relPath =  path.relative(basePath, target.pathname );

  if (relPath.startsWith('.')) return relPath;
  return `./${relPath}`
}
