import { mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { basename } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { fsa } from '@chunkd/fs';
import { logger, stringToUrlFolder, Url, UrlFolder, gitContext } from '@linzjs/topographic-system-shared';
import { command, option, optional, restPositionals, string } from 'cmd-ts';
import { $ } from 'zx';

type GeoJson = { type: string; features: unknown[] };
/**
 * Max characters of GeoJSON to embed in the summary. The summary is posted as a GitHub PR comment,
 * which the API rejects over 65536 characters (see MAX_COMMENT_SIZE in the shared GithubApi). This
 * is kept well under that so the combined preview, the per-dataset section, and the diff snippets
 * all fit together with margin to spare.
 */
export const MaxGeoJsonLength = 25_000;
export const MaxDiffLines = 30;
export const MaxDiffLineLength = 120;
/**
 * Above this many changed features, skip reading the diff into memory entirely. This guards against
 * ERR_STRING_TOO_LONG (kart can emit diffs larger than V8's ~512MB max string length); the GitHub
 * comment size is handled separately by {@link MaxGeoJsonLength}.
 */
export const MaxFeatureCount = 1_000;

interface GitContext {
  /** Repository context path to operate on eg "repo" */
  repo: URL;

  diffRange: string[];

  /** Location to output files to */
  output: URL;
}

/** Canonical filenames for the diff artifacts written under {@link GitContext.output}. */
const TextDiffName = 'kart_diff.txt';
const HtmlDiffName = 'kart_diff.html';
const GeojsonDiffName = 'kart_diff.geojson';
const GitDiffName = 'git_diff.txt';

/** Run kart to write the text diff artifact to disk, returning its location. */
async function writeTextDiff(ctx: GitContext): Promise<URL> {
  mkdirSync(ctx.output, { recursive: true });
  const location = new URL(TextDiffName, ctx.output);
  await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o text --output "${fileURLToPath(location)}"`;
  return location;
}

/** Run kart to write the html diff artifact to disk, returning its location. */
async function writeHtmlDiff(ctx: GitContext): Promise<URL> {
  mkdirSync(ctx.output, { recursive: true });
  const location = new URL(HtmlDiffName, ctx.output);
  // Output to `-` (stdout) disables opening a browser when running locally.
  // Piping stdout directly to file (`>`) avoids `kart` adding pagination.
  await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o html --output - > "${fileURLToPath(location)}"`;
  return location;
}

/** Run kart to write the geojson diff artifact to disk, returning its location. */
async function writeGeojsonDiff(ctx: GitContext): Promise<URL> {
  mkdirSync(ctx.output, { recursive: true });
  const location = new URL(GeojsonDiffName, ctx.output);
  await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o geojson --output "${fileURLToPath(location)}"`;
  return location;
}

/**
 * Run git (not kart) to write the underlying git diff to disk, returning its location. Temporarily
 * moves the kart index aside so git reports the raw object changes. Written via redirect (not stdout
 * capture) so an oversized diff can't crash with ERR_STRING_TOO_LONG.
 */
async function writeGitDiff(ctx: GitContext): Promise<URL> {
  mkdirSync(ctx.output, { recursive: true });
  const location = new URL(GitDiffName, ctx.output);
  const tempIndexPath = fileURLToPath(new URL('.kart/no.index', ctx.repo));
  const indexPath = fileURLToPath(new URL('.kart/index', ctx.repo));
  await $`mv ${indexPath} ${tempIndexPath}`;
  try {
    await $`git ${gitContext(ctx.repo)} diff --no-color ${ctx.diffRange} > "${fileURLToPath(location)}"`;
  } finally {
    await $`mv ${tempIndexPath} ${indexPath}`;
  }
  return location;
}

async function readTextDiff(ctx: GitContext): Promise<string> {
  try {
    const fileContent = await readFileWithRetry(new URL(TextDiffName, ctx.output));
    const textDiff = fileContent.toString('utf-8');
    logger.info({ textDiffLines: textDiff.split('\n').length }, 'Diff:TextDiffSaved');
    return textDiff;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:TextDiffFailed');
    throw error;
  }
}

type FeatureCountOutput = Record<string, number>;

export function sumFeatureCounts(output: FeatureCountOutput): number {
  return Object.values(output).reduce((total, count) => total + (typeof count === 'number' ? count : 0), 0);
}

async function getFeatureCount(ctx: GitContext): Promise<FeatureCountOutput> {
  try {
    const countOutput = await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o json --only-feature-count exact`;
    const stdout = countOutput.stdout.trim();
    if (stdout === '') {
      logger.warn('Diff:FeatureCount:EmptyOutput');
      return {};
    }
    let changes: FeatureCountOutput;
    try {
      changes = JSON.parse(stdout) as FeatureCountOutput;
    } catch (e) {
      logger.error({ error: e, stdout }, 'Diff:FeatureCount:JSONParseError');
      return {};
    }
    logger.info({ ...ctx, totalFeaturesChanged: sumFeatureCounts(changes) }, 'Diff:FeatureCount');
    return changes;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:Feature count failed');
    throw error;
  }
}

/**
 * Generate all raw diff files on disk (text, html, geojson, git) without reading them back into
 * memory. Always run so the artifacts are downloadable even when there is no summary to post (no
 * features changed, or the diff is too large to load into a JS string).
 */
async function writeDiffArtifacts(ctx: GitContext): Promise<void> {
  await writeTextDiff(ctx);
  await writeHtmlDiff(ctx);
  await writeGeojsonDiff(ctx);
  await writeGitDiff(ctx);
  logger.info({ output: ctx.output }, 'Diff:ArtifactsWritten');
}

/**
 * Read the already-written html artifact, unescape kart's hex sequences, and rewrite it in place.
 * FIXME: This is required as of kart version 16. Check future versions if it was fixed upstream.
 **/
async function fixHtmlDiff(ctx: GitContext): Promise<void> {
  try {
    const htmlDiffLocation = new URL(HtmlDiffName, ctx.output);
    const content = await readFileWithRetry(htmlDiffLocation);
    const fixedContent = content.toString('utf-8').replace(/\\x2f/g, '/').replace(/\\x3c/g, '<').replace(/\\x3e/g, '>');
    await fsa.write(htmlDiffLocation, fixedContent);
    logger.info({ htmlDiffLines: fixedContent.split('\n').length }, 'Diff:HtmlDiffSaved');
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:HtmlDiffFailed');
    throw error;
  }
}

async function readGeojsonFile(file: URL): Promise<{ datasetName: string; fileString: string } | undefined> {
  const datasetName = basename(file.href, '.geojson');
  const fileContent = await readFileWithRetry(file);
  const fileString = fileContent.toString('utf-8');
  const geojson = JSON.parse(fileString) as GeoJson;
  if (geojson.type === 'FeatureCollection' && Array.isArray(geojson.features)) {
    return { datasetName, fileString };
  }
  return undefined;
}

async function readGeojsonDiff(ctx: GitContext): Promise<Record<string, string>> {
  try {
    const geojsonDiffLocation = new URL(GeojsonDiffName, ctx.output);
    const stat = await fsa.head(geojsonDiffLocation);
    const geojsonByDataset: Record<string, string> = {};

    if (stat && stat.isDirectory) {
      const files = await fsa.toArray(fsa.list(geojsonDiffLocation, { recursive: true }));
      for (const file of files) {
        const jsonFile = await readGeojsonFile(file);
        if (jsonFile) {
          geojsonByDataset[jsonFile.datasetName] = jsonFile.fileString;
        }
      }
    } else if (stat) {
      const jsonFile = await readGeojsonFile(geojsonDiffLocation);
      if (jsonFile) {
        geojsonByDataset[jsonFile.datasetName] = jsonFile.fileString;
      }
    }
    logger.info({ geojsonDiffLocation }, 'Diff:GeoJsonDiffLoaded');
    return geojsonByDataset;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:GeoJsonDiffFailed');
    throw error;
  }
}

async function readGitDiff(ctx: GitContext): Promise<string> {
  try {
    const fileContent = await readFileWithRetry(new URL(GitDiffName, ctx.output));
    const gitDiff = fileContent.toString('utf-8');
    logger.info({ gitDiffLines: gitDiff.split('\n').length }, 'Diff:GitDiffSaved');
    return gitDiff;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:GitDiffFailed');
    throw error;
  }
}

export async function readFileWithRetry(filePath: URL, retries = 5, delay = 10): Promise<Buffer> {
  for (let i = 0; i < retries; i++) {
    try {
      return await fsa.read(filePath);
    } catch {
      if (i === retries - 1) throw new Error(`Failed to read file ${filePath.href} after ${retries} retries`);
      await new Promise((resolve) => setTimeout(resolve, delay * Math.pow(2, i)));
    }
  }
  throw new Error(`Failed to read file ${filePath.href} after ${retries} retries`);
}

export const DiffCommand = command({
  name: 'diff',
  description: 'Run specified diff commands on a cloned kart repository',
  args: {
    context: option({
      type: optional(UrlFolder),
      long: 'context',
      short: 'C',
      description:
        'Run as if git was started in <path> instead of the current working directory see git -C for more details',
    }),
    output: option({
      type: UrlFolder,
      long: 'output',
      description: 'Optional output directory for diff results (default: $TMPDIR/kart/diff)',
      defaultValue: () => stringToUrlFolder(path.join(tmpdir(), 'kart', 'diff')),
    }),
    summaryFile: option({
      type: Url,
      long: 'summary-file',
      description: 'Optional output file for summary markdown (default: pr_summary.md)',
      defaultValue: () => pathToFileURL('pr_summary.md'),
    }),
    diff: restPositionals({
      description: 'Commit SHA or branches to diff (default: master..FETCH_HEAD)',
      type: string,
    }),
  },
  async handler(args) {
    logger.info({ ref: args.diff }, 'Diff:Start');

    const ctx: GitContext = {
      repo: args.context ?? stringToUrlFolder('repo'),
      diffRange: [],
      output: args.output,
    };
    if (args.diff.length > 0) {
      ctx.diffRange = args.diff;
    } else {
      try {
        // First try to find the merge base between master and FETCH_HEAD
        const mergeBase = await $`git ${gitContext(ctx.repo)} merge-base origin/master FETCH_HEAD`;
        const baseCommit = mergeBase.stdout.trim();
        ctx.diffRange = [`${baseCommit}..FETCH_HEAD`];
        logger.info({ baseCommit, diffRange: ctx.diffRange }, 'Diff:Using merge-base strategy');
      } catch (mergeBaseError) {
        // If merge-base fails, fall back to comparing against origin/master directly
        logger.warn({ error: mergeBaseError }, 'Diff:Merge-base failed, falling back to direct comparison');
        ctx.diffRange = ['origin/master', 'FETCH_HEAD'];
      }
    }

    logger.info(ctx, 'Diff:UsingRange');

    try {
      const featureCounts = await getFeatureCount(ctx);
      const featureCount = sumFeatureCounts(featureCounts);

      if (featureCount > MaxFeatureCount) {
        logger.warn({ featureCount, maxFeatureCount: MaxFeatureCount }, 'Diff:TooLargeToPreview');
        await fsa.write(args.summaryFile, buildTooLargeSummary(featureCounts));
        logger.info('Diff:CommandCompleted');
        return;
      }

      await writeDiffArtifacts(ctx);

      if (featureCount === 0) {
        logger.info({ ...ctx }, 'Diff:NoFeatureChanges');
        return;
      }

      const textDiff = await readTextDiff(ctx);
      await fixHtmlDiff(ctx);

      const geojsonByDataset = await readGeojsonDiff(ctx);
      const gitDiff = await readGitDiff(ctx);

      const summaryMd = buildMarkdownSummary(featureCounts, textDiff, gitDiff, geojsonByDataset);
      logger.info('Diff:SummaryBuilt');
      await fsa.write(args.summaryFile, summaryMd);
      logger.info('Diff:CommandCompleted');
    } catch (error) {
      logger.error({ error, ...ctx }, 'Diff:Failed');
      throw error;
    }
  },
});

export function truncateDiffLines(
  diff: string,
  maxLines: number,
  maxLineLength: number,
): { text: string; truncated: boolean; totalLines: number } {
  const lines = diff.split('\n');
  const totalLines = lines.length;
  const truncatedLineCount = totalLines > maxLines;

  const text = lines
    .slice(0, maxLines)
    .map((line) => (line.length > maxLineLength ? line.slice(0, maxLineLength) + '…' : line))
    .join('\n');

  return { text, truncated: truncatedLineCount, totalLines };
}

/**
 * Build a minimal summary for diffs too large to load into memory. The full diff is not read, so we
 * surface only the per-dataset counts (cheaply obtained from `--only-feature-count`) and point at
 * the workflow artifacts for the detail.
 */
export function buildTooLargeSummary(featureCounts: FeatureCountOutput): string {
  const total = sumFeatureCounts(featureCounts);
  const datasets = Object.entries(featureCounts)
    .filter(([, count]) => count > 0)
    .sort(([, a], [, b]) => b - a);

  let summary = `# Changes Summary\n\n`;
  summary += `**Total Features Changed**: ${total}\n`;
  summary += `**Datasets Affected**: ${datasets.length}\n\n`;
  summary += `## Feature Changes Preview\n`;
  summary += `*Too many features changed (${MaxFeatureCount} limit) to preview. `;
  summary += `Check workflow artifacts for the full diff.*\n\n`;
  if (datasets.length > 0) {
    summary += `### Changes by Dataset\n\n`;
    for (const [dataset, count] of datasets) {
      summary += `- **${dataset}**: ${count} features changed\n`;
    }
    summary += '\n';
  }
  return summary;
}

export function buildMarkdownSummary(
  featureCounts: FeatureCountOutput,
  textDiff: string,
  gitDiff: string,
  geojsonByDataset: Record<string, string>,
): string {
  const gitDiffInfo = truncateDiffLines(gitDiff, MaxDiffLines, MaxDiffLineLength);
  const textDiffInfo = truncateDiffLines(textDiff, MaxDiffLines, MaxDiffLineLength);

  const total = sumFeatureCounts(featureCounts);
  const datasets = Object.entries(featureCounts)
    .filter(([, count]) => count > 0)
    .sort(([, a], [, b]) => b - a);

  let summary = `# Changes Summary\n\n`;
  summary += `**Total Features Changed**: ${total}\n`;
  summary += `**Datasets Affected**: ${datasets.length}\n`;
  summary += `**Git Diff Lines**: ${gitDiffInfo.totalLines}\n\n`;

  const allFeatures = Object.values(geojsonByDataset)
    .map((geojsonStr) => (JSON.parse(geojsonStr) as GeoJson).features)
    .flat();

  const allChangesGeoJson = JSON.stringify({ type: 'FeatureCollection', features: allFeatures }, null, 2);

  // Only embed GeoJSON if it is under the character limit (keeps the comment within GitHub's size cap)
  const canEmbedGeojson = allChangesGeoJson.length <= MaxGeoJsonLength;
  if (allFeatures.length > 0) {
    summary += `## Feature Changes Preview\n`;
    if (canEmbedGeojson) {
      summary += '```geojson\n';
      summary += `${allChangesGeoJson}\n`;
      summary += '```\n\n';
    } else {
      summary += `*GeoJSON too large to display (${allChangesGeoJson.length} characters > ${MaxGeoJsonLength} limit). `;
      summary += `Check workflow artifacts for full GeoJSON data.*\n\n`;
    }
  }

  if (datasets.length > 0 && canEmbedGeojson) {
    summary += `### Changes by Dataset\n\n`;
    for (const [dataset, count] of datasets) {
      summary += `<details>\n<summary>**${dataset}**: ${count} features changed</summary>\n\n`;
      const geojsonStr = geojsonByDataset[dataset];
      if (geojsonStr) {
        summary += '```geojson\n';
        summary += `${geojsonStr}\n`;
        summary += '```\n';
      }
      summary += `</details>\n\n`;
    }
    summary += '\n';
  }

  return summary + buildDiffSections(total, textDiffInfo, gitDiffInfo);
}

type DiffInfo = ReturnType<typeof truncateDiffLines>;

/** Render the collapsible Kart Diff and Git Diff sections shared by every summary. */
function buildDiffSections(featureCount: number, textDiffInfo: DiffInfo, gitDiffInfo: DiffInfo): string {
  let summary = `## Kart Diff\n\n`;
  summary += `<details>\n`;
  summary += `<summary>Kart Diff (${featureCount} features)</summary>\n\n`;
  summary += '```diff\n';
  summary += `${textDiffInfo.text}\n`;
  if (textDiffInfo.truncated) {
    summary += `\n... truncated (showing ${MaxDiffLines} of ${textDiffInfo.totalLines} lines)\n`;
  }
  summary += '```\n';
  summary += `</details>\n\n`;

  summary += `## Git Diff\n\n`;
  summary += `<details>\n`;
  summary += `<summary>Git Diff (${gitDiffInfo.totalLines} lines)</summary>\n\n`;
  summary += '```diff\n';
  summary += `${gitDiffInfo.text}\n`;
  if (gitDiffInfo.truncated) {
    summary += `\n... truncated (showing ${MaxDiffLines} of ${gitDiffInfo.totalLines} lines)\n`;
  }
  summary += '```\n';
  summary += `</details>\n\n`;

  return summary;
}
