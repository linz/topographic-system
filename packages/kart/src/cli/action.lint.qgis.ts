import { fsa } from '@chunkd/fs';
import { logger, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option, optional, restPositionals } from 'cmd-ts';
import { XMLParser } from 'fast-xml-parser';

export const LintQgisProjectArgs = {
  qgis: option({
    type: optional(Url),
    long: 'qgis',
    description: 'Path to QGIS project file',
  }),
  paths: restPositionals({
    type: Url,
    description: 'QGIS projects to lint',
  }),
};
type LintRule = (node: Record<string, unknown>) => string | null;

/**
 * List of Fonts to allow with their namedStyles
 */
const AllowedFonts: Record<string, boolean | Set<string>> = {
  "Nimbus Sans Narrow": new Set(["default", "Oblique", "Regular", "Bold"]),
  "Nimbus Sans": new Set(["default", "Regular", "Bold", "Italic", "Bold Italic"])
}

const LintRules: LintRule[] = [
  lintDataSources,
  lintFontFamilies,
]

export const LintQgisProjectCommand = command({
  name: 'lint-qgis',
  description: 'Lint QGIS Project',
  args: LintQgisProjectArgs,
  async handler(args) {
    registerFileSystem();

    logger.info({ args }, 'LintQgis:Start');

    const startTime = performance.now();

      for (const path of [...args.paths, args.qgis]) {
        if (path == null) continue;
      
      const qgisFile = await fsa.read(path);
      const parser = new XMLParser({ ignoreAttributes: false, processEntities: false });
      const qgisXml = parser.parse(qgisFile);
      const errors = lint(qgisXml,LintRules );

      if (errors.length > 0) {
        for (const error of errors) logger.error({ error }, 'LintQgis:Error');
        throw new Error(`QGIS project lint failed with ${errors.length} error(s):\n${errors.join('\n')}`);
      }
  }
    logger.info({duration: performance.now() - startTime}, 'LintQgis:Completed');
  },
});

export function lint(node: unknown, rules: LintRule[], visited = new Set<unknown>(), errors: string[] = []): string[] {
  if (node == null) return errors;
  if (visited.has(node)) {
    console.warn('Already visited node, skipping to avoid circular reference', { node });
    return errors;
  }

  for (const rule of rules) {
    const error = rule(node as Record<string, unknown>);
    if (error != null) errors.push(error);
  }


  visited.add(node);
    for (const value of Object.values(node)) {
      if (value == null) continue;
      if (typeof value !== 'object') continue;
      lint(value, rules, visited, errors);
  }

  return errors;
}




export function lintFontFamilies(node: Record<string, unknown>): string | null {
  const fontFamily = node['@_fontFamily'] as string | undefined;
  if (fontFamily == null) return null;

  const fontConfig = AllowedFonts[fontFamily];
  if (fontConfig === true) return null; // All styles of this font are allowed
  if (fontConfig instanceof Set) {
    // Default "" and null to "default"
    const fontStyle = (node['@_namedStyle'] || 'default') as string;
    if (fontConfig.has(fontStyle)) return null; // This style of the font is allowed
    return `Font Style "${fontFamily}" does not allow '${fontStyle}'. Allowed style are: ${Array.from(fontConfig).join(', ')}`;
  }

  return `Font family "${fontFamily}" is not allowed. Allowed fonts are: ${Object.keys(AllowedFonts).join(', ')}`;
}

/**
 * Ensure all datasource paths in the QGIS project are relative paths. 
 * Absolute paths or non-local paths will cause issues when the project is used on a different machine or environment.
 * @param node 
 * @returns 
 */
export function lintDataSources(node: Record<string, unknown>): string | null {
  const dataSource = node['datasource'] as string | undefined;
  if (dataSource == null || dataSource === "") return null;
  const provider = node['provider'] as string | undefined;
  if (provider == null) return null;
  if (provider !== 'ogr') return null; 

    if (!(dataSource.startsWith('./') || dataSource.startsWith('../'))) {
      return `datasource path must be relative (start with ./ or ../): ${dataSource}`;
    }
  
  return null
}
