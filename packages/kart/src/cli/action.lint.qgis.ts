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

export interface LintContext {
  qgisPath: URL;
}

type LintRule = (node: Record<string, unknown>, context: LintContext) => string | null | Promise<string | null>;
type LintRuleContext = { name: string; rule: LintRule };

/**
 * List of Fonts to allow with their namedStyles
 */
const AllowedFonts: Record<string, true | Set<string>> = {
  'Nimbus Sans LINZ': new Set([
    'default',
    'Regular',
    'Bold',
    'Italic',
    'Bold Italic',
    'Narrow',
    'Narrow Italic',
    'Narrow Bold',
    'Narrow Bold Italic',
  ]),
};

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
      const errors = await lint(qgisFile, LintRules, { qgisPath: path });

      if (errors.length > 0) {
        for (const error of errors) logger.error({ rule: error.name, error: error.error }, 'LintQgis:Error');
        throw new Error(
          `QGIS project lint failed with ${errors.length} error(s):\n${errors.map((e) => `${e.name}: ${e.error}`).join('\n')}`,
        );
      }
    }
    logger.info({ duration: performance.now() - startTime }, 'LintQgis:Completed');
  },
});

export async function lint(
  obj: string | Buffer | Record<string, unknown>,
  rules: LintRuleContext[],
  context: LintContext,
): Promise<{ name: string; error: string }[]> {
  let errors: { name: string; error: string }[];
  if (typeof obj === 'string' || Buffer.isBuffer(obj)) {
    const parser = new XMLParser({ ignoreAttributes: false, processEntities: false });
    const qgisXml = parser.parse(obj);
    errors = await doLint(qgisXml, rules, [], context);
  } else {
    errors = await doLint(obj, rules, [], context);
  }

  // Deduplicate by name + error combination
  const seen = new Set<string>();
  return errors.filter((e) => {
    const key = `${e.name}:${e.error}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export async function doLint(
  node: unknown,
  rules: LintRuleContext[],
  errors: { name: string; error: string }[],
  context: LintContext,
): Promise<{ name: string; error: string }[]> {
  if (node == null) return errors;

  for (const rule of rules) {
    const error = await rule.rule(node as Record<string, unknown>, context);
    if (error != null) errors.push({ name: rule.name, error });
  }

  for (const value of Object.values(node)) {
    if (value == null) continue;
    if (typeof value !== 'object') continue;
    await doLint(value, rules, errors, context);
  }

  return errors;
}
export const LintRuleFontFamily: LintRuleContext = {
  name: 'font-families',
  rule(node) {
    const fontFamily = node['@_fontFamily'] as string | undefined;
    if (fontFamily == null) return null;

    const fontConfig = AllowedFonts[fontFamily];
    if (fontConfig == null) {
      return `Font family '${fontFamily}' is not allowed. Allowed fonts are: ${Object.keys(AllowedFonts).join(', ')}`;
    }

    if (fontConfig === true) return null; // All styles of this font are allowed

    // Default "" and null to "default"
    const fontStyle = (node['@_namedStyle'] || 'default') as string;
    if (fontConfig.has(fontStyle)) return null; // This style of the font is allowed
    return `Font Style '${fontFamily}' does not allow '${fontStyle}'. Allowed style are: ${Array.from(fontConfig).join(', ')}`;
  },
};
/**
 * Ensure all datasource paths in the QGIS project are relative paths.
 * Absolute paths or non-local paths will cause issues when the project is used on a different machine or environment.
 * @param node
 * @returns
 */
export const LintRuleDataSources: LintRuleContext = {
  name: 'data-sources',
  rule(node) {
    const dataSource = node['datasource'] as string | undefined;
    if (dataSource == null || dataSource === '') return null;
    const provider = node['provider'] as string | undefined;
    if (provider == null) return null;
    if (provider !== 'ogr') return null;

    if (!(dataSource.startsWith('./') || dataSource.startsWith('../'))) {
      return `datasource path must be relative (start with ./ or ../): ${dataSource}`;
    }

    return null;
  },
};
/**
 * Helper to recursively extract SVG file paths from an SvgFill symbol layer node.
 */
export function findSvgPaths(node: Record<string, unknown>): Set<string> {
  const paths: Set<string> = new Set();

  function find(obj: unknown): void {
    if (obj == null || typeof obj !== 'object') return;

    if (Array.isArray(obj)) {
      for (const item of obj) find(item);
      return;
    }

    const rec = obj as Record<string, unknown>;
    if (rec !== node && typeof rec['@_class'] === 'string') return;

    const key = (rec['@_name'] ?? rec['@_k']) as string | undefined;
    const val = (rec['@_value'] ?? rec['@_v']) as string | undefined;
    if (typeof key === 'string' && typeof val === 'string' && val.trim() !== '') {
      const k = key.toLowerCase();
      const v = val.trim();
      if (k === 'svgfile' || k === 'svg_path' || k === 'file' || (k === 'name' && v.endsWith('.svg'))) {
        paths.add(v);
      }
    }

    for (const value of Object.values(rec)) {
      if (value != null && typeof value === 'object') {
        find(value);
      }
    }
  }

  find(node);
  return paths;
}

/**
 * Ensure all SVG file paths referenced in SVG Fills exist.
 * @param node
 * @param context
 */
// export async function lintSvgFills
export const LintRuleSvgFills: LintRuleContext = {
  name: 'svg-fill',
  async rule(node, context) {
    const className = node['@_class'] as string | undefined;
    if (typeof className !== 'string' || className.toLowerCase() !== 'svgfill') return null;

    const svgPaths = findSvgPaths(node);
    if (svgPaths.size === 0) return null;

    const missingFiles: string[] = [];
    for (const svgPath of svgPaths) {
      if (svgPath.startsWith('base64:')) continue;

      const targetUrl = new URL(svgPath, context.qgisPath);

      const exists = await fsa.exists(targetUrl);
      if (!exists) missingFiles.push(svgPath);
    }

    if (missingFiles.length > 0) return `SVG Fill file does not exist: ${missingFiles.join(', ')}`;

    return null;
  },
};

export const LintRules: LintRuleContext[] = [LintRuleDataSources, LintRuleFontFamily, LintRuleSvgFills];
