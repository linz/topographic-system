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
    const fontFamily = X.string(node, '@_fontFamily');
    if (fontFamily == null) return null;

    const fontConfig = AllowedFonts[fontFamily];
    if (fontConfig == null) {
      return `Font family '${fontFamily}' is not allowed. Allowed fonts are: ${Object.keys(AllowedFonts).join(', ')}`;
    }

    if (fontConfig === true) return null; // All styles of this font are allowed

    // Default "" and null to "default"
    const fontStyle = X.string(node, '@_namedStyle') || 'default';
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
    const dataSource = X.string(node, 'datasource');
    if (dataSource == null || dataSource.trim() === '') return null;
    const provider = X.string(node, 'provider');
    if (provider !== 'ogr') return null;

    if (!(dataSource.startsWith('./') || dataSource.startsWith('../'))) {
      return `datasource path must be relative (start with ./ or ../): ${dataSource}`;
    }

    return null;
  },
};
/**
 * Helper to recursively extract SVG file paths from an SVGFill | SvgMarker symbol layer node.
 */
export function findSvgPaths(obj: Record<string, unknown>, paths = new Set<string>()): Set<string> {
  if (obj == null || typeof obj !== 'object') return paths;

  if (Array.isArray(obj)) {
    for (const item of obj) findSvgPaths(item, paths);
    return paths;
  }

  const rec = obj as Record<string, unknown>;
  for (const value of Object.values(rec)) {
    if (value == null) continue;
    if (typeof value === 'object') findSvgPaths(value as Record<string, unknown>, paths);
    if (typeof value === 'string') {
      if (value.endsWith('.svg')) paths.add(value);
    }
  }
  return paths;
}

/** Xml helper utils */
const X = {
  /** Attempt to read a string value from the xml  */
  string(node: Record<string, unknown>, key: string): string | undefined {
    const value = node[key];
    if (typeof value === 'string') return value;
    return undefined;
  },
};

/**
 * Ensure all SVG file paths referenced in SVG Fills exist.
 * @param node
 * @param context
 */
export const LintRuleSvgPath: LintRuleContext & { classes: Set<string> } = {
  name: 'svg-path',
  classes: new Set(['svgfill', 'svgmarker']),
  async rule(node, context) {
    const className = X.string(node, '@_class') ?? '';
    if (!this.classes.has(className?.toLowerCase())) return null;

    const svgPaths = findSvgPaths(node);
    if (svgPaths.size === 0) return null;

    const missingFiles: string[] = [];
    for (const svgPath of svgPaths) {
      if (svgPath.startsWith('base64:')) continue;

      const targetUrl = new URL(svgPath, context.qgisPath);
      const exists = await fsa.exists(targetUrl);
      if (!exists) missingFiles.push(svgPath);
    }

    if (missingFiles.length > 0) return `${className} file does not exist: "${missingFiles.join(', ')}"`;

    return null;
  },
};

export const LintRules: LintRuleContext[] = [LintRuleDataSources, LintRuleFontFamily, LintRuleSvgPath];
