import { relative } from 'node:path';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { logger, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { getDataFromCatalog } from '@linzjs/topographic-system-stac';
import { command, option, optional, restPositionals } from 'cmd-ts';
import { XMLParser } from 'fast-xml-parser';

const isGithubActions = () => process.env['GITHUB_ACTIONS'] === 'true';

export const LintOk = Symbol('Ok');
type LintRuleOk = typeof LintOk;

export const LintQgisProjectArgs = {
  catalog: option({
    type: optional(Url),
    long: 'catalog',
    description: 'Path or URL to root STAC catalog where dataset source parquet exists',
  }),
  qgis: option({ type: optional(Url), long: 'qgis', description: 'Path to QGIS project file' }),
  paths: restPositionals({ type: Url, description: 'QGIS projects to lint' }),
};

export interface LintContext {
  qgisPath: URL;
  catalog?: URL;
  validLayers?: Map<string, string | LintRuleOk>;
}

type LintRule = (
  node: Record<string, unknown>,
  context: LintContext,
) => string | LintRuleOk | Promise<string | LintRuleOk>;
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

    let totalErrors: string[] = [];
    logger.info({ args }, 'LintQgis:Start');

    const startTime = performance.now();
    const validLayers = new Map<string, string | LintRuleOk>();

    for (const path of [...args.paths, args.qgis]) {
      if (path == null) continue;

      const qgisFile = await fsa.read(path);
      const errors = await lint(qgisFile, LintRules, { qgisPath: path, catalog: args.catalog, validLayers });

      if (errors.length > 0) {
        for (const error of errors) {
          logger.error({ file: path, rule: error.name, error: error.error }, 'LintQgis:Error');
          if (isGithubActions()) emitGithubAnnotation(path, error);
        }
        totalErrors.push(`${path.toString()}: ${errors.length} error(s)`);
      }
    }

    if (totalErrors.length > 0) {
      throw new Error(`QGIS project lint failed with ${totalErrors.length} error(s):\n${totalErrors.join('\n')}`);
    }
    logger.info({ duration: performance.now() - startTime }, 'LintQgis:Completed');
  },
});

export async function lint(
  obj: string | Buffer | Record<string, unknown>,
  rules: LintRuleContext[],
  context: LintContext,
): Promise<{ name: string; error: string }[]> {
  context.validLayers ??= new Map();
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
    if (error !== LintOk) errors.push({ name: rule.name, error });
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
    if (fontFamily == null) return LintOk;

    const fontConfig = AllowedFonts[fontFamily];
    if (fontConfig == null) {
      return `Font family '${fontFamily}' is not allowed. Allowed fonts are: ${Object.keys(AllowedFonts).join(', ')}`;
    }

    if (fontConfig === true) return LintOk; // All styles of this font are allowed

    // Default "" and null to "default"
    const fontStyle = X.string(node, '@_namedStyle') || 'default';
    if (fontConfig.has(fontStyle)) return LintOk; // This style of the font is allowed
    return `Font Style '${fontFamily}' does not allow '${fontStyle}'. Allowed style are: ${Array.from(fontConfig).join(', ')}`;
  },
};
/**
 * Ensure all datasource paths in the QGIS project are relative paths.
 * Absolute paths or non-local paths will cause issues when the project is used on a different machine or environment.
 * @param node
 * @returns
 */
export interface ParsedDataSource {
  path: string;
  name: string;
  extras: string[];
}

/**
 * Extract the path, layer name and any piped extras from a QGIS datasource string.
 */
export function parseDataSource(dataSource: string): ParsedDataSource {
  const [path, ...extras] = (dataSource ?? '').split('|');
  const fileName = path?.split('/').pop() ?? '';
  const name = fileName.replace(/\.(parquet|geojson|gpkg|json)$/, '');
  return { path: path ?? '', name, extras };
}

export const LintRuleDataSources: LintRuleContext = {
  name: 'data-sources',
  async rule(node, context) {
    const dataSource = X.string(node, 'datasource');
    if (dataSource == null || dataSource.trim() === '') return LintOk;
    const provider = X.string(node, 'provider');
    if (provider !== 'ogr') return LintOk;

    const { path, name } = parseDataSource(dataSource);

    if (!path.startsWith('./')) {
      return `datasource path must be relative (start with ./): ${dataSource}`;
    }

    if (context.catalog == null) return LintOk;
    if (name === '') return LintOk;

    const seen = context.validLayers?.get(name);
    if (seen != null) return seen;

    const loc = await getDataFromCatalog(context.catalog, name).catch(() => null);
    const ruleResult =
      loc == null ? `datasource layer '${name}' not found in catalog: ${context.catalog.href}` : LintOk;

    context.validLayers?.set(name, ruleResult);
    return ruleResult;
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
    if (!this.classes.has(className?.toLowerCase())) return LintOk;

    const svgPaths = findSvgPaths(node);
    if (svgPaths.size === 0) return LintOk;

    const missingFiles: string[] = [];
    for (const svgPath of svgPaths) {
      if (svgPath.startsWith('base64:')) continue;

      const targetUrl = new URL(svgPath, context.qgisPath);
      const exists = await fileExists(targetUrl);
      if (!exists) missingFiles.push(svgPath);
    }

    if (missingFiles.length > 0) return `${className} file does not exist: "${missingFiles.join(', ')}"`;

    return LintOk;
  },
};

async function fileExists(url: URL): Promise<boolean> {
  try {
    const exists = await fsa.exists(url);
    return exists;
  } catch {
    // noop
  }
  return false;
}

export const LintRules: LintRuleContext[] = [LintRuleDataSources, LintRuleFontFamily, LintRuleSvgPath];

export function toGithubPath(url: URL): string {
  if (url.protocol === 'file:') {
    const rootDir = process.env['GITHUB_WORKSPACE'] ?? process.cwd();
    return relative(rootDir, fileURLToPath(url));
  }
  return url.toString();
}

export function emitGithubAnnotation(path: URL, error: { name: string; error: string }): void {
  console.log(`::error file=${toGithubPath(path)},title=${escapeProperty(error.name)}::${escapeData(error.error)}`);
}

// Stolen from @actions/core
function escapeProperty(val: string): string {
  return val.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A').replace(/:/g, '%3A').replace(/,/g, '%2C');
}
function escapeData(val: string): string {
  return val.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
}
