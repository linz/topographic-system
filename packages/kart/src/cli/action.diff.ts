import type { UUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { basename } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { fsa } from '@chunkd/fs';
import { logger, stringToUrlFolder, Url, UrlFolder, gitContext } from '@linzjs/topographic-system-shared';
import { command, option, optional, restPositionals, string } from 'cmd-ts';
import { $ } from 'zx';

type GeoJson = { type: string; features: unknown[] };
type DiffOutput = {
  [kartFormat: string]: {
    [dataset: string]: {
      feature: Array<{
        [changeType in '+' | '++' | '-' | '--']?: {
          topo_id: UUID | null;
          t50_fid: number | null;
          feature_type?: string | null;
          name?: string | null;
          theme?: string | null;
          source?: string | null;
          source_date?: string | null;
          capture_method?: string | null;
          change_type?: string | null;
          update_date?: string | null;
          create_date?: string | null;
          version?: number | null;
          geom: string;
          [key: string]: unknown;
        };
      }>;
    };
  };
};
const MAX_GEOJSON_LENGTH = 25_000;

interface GitContext {
  /** Repository context path to operate on eg "repo" */
  repo: URL;

  diffRange: string[];

  /** Location to output files to */
  output: URL;
}

async function getTextDiff(ctx: GitContext): Promise<string> {
  try {
    const textDiffLocation = new URL('kart_diff.txt', ctx.output);
    const processOutput =
      await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o text --output "${fileURLToPath(textDiffLocation)}"`;
    logger.debug({ stdout: processOutput.stdout, stderr: processOutput.stderr }, 'Diff:KartTextDiffConsoleOutput');
    const fileContent = await readFileWithRetry(textDiffLocation);
    const textDiff = fileContent.toString('utf-8');
    logger.info({ textDiffLines: textDiff.split('\n').length }, 'Diff:TextDiffSaved');
    return textDiff;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:TextDiffFailed');
    throw error;
  }
}

async function getFeatureCount(ctx: GitContext): Promise<number> {
  try {
    const countOutput = await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o json`;
    const featureCount = countOutput.stdout.trim();
    if (featureCount == null) {
      logger.warn('Diff:FeatureCount:EmptyOutput');
      return 0;
    }
    let totalFeaturesChanged = 0;
    let changes: DiffOutput;
    try {
      changes = JSON.parse(featureCount) as DiffOutput;
    } catch (e) {
      logger.error({ error: e, featureCount }, 'Diff:FeatureCount:JSONParseError');
      return 0;
    }
    for (const formatKey of Object.keys(changes)) {
      const dataset = changes[formatKey];
      if (dataset == null) continue;
      for (const datasetKey of Object.keys(dataset)) {
        if (dataset[datasetKey] == null) continue;
        totalFeaturesChanged += dataset[datasetKey]?.feature?.length ?? 0;
      }
    }
    logger.info({ ...ctx, totalFeaturesChanged }, 'Diff:FeatureCount');
    return totalFeaturesChanged;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:Feature count failed');
    throw error;
  }
}

async function createHtmlDiff(ctx: GitContext): Promise<URL> {
  try {
    const htmlDiffLocation = new URL('kart_diff.html', ctx.output);
    await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o html --output "${fileURLToPath(htmlDiffLocation)}"`;
    const content = await readFileWithRetry(htmlDiffLocation);
    const fixedContent = content.toString('utf-8').replace(/\\x2f/g, '/').replace(/\\x3c/g, '<').replace(/\\x3e/g, '>');
    await fsa.write(htmlDiffLocation, fixedContent);
    return htmlDiffLocation;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:HTML diff failed');
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

async function createGeojsonDiff(ctx: GitContext): Promise<Record<string, string>> {
  try {
    const geojsonDiffLocation = new URL('kart_diff.geojson', ctx.output);
    await $`kart ${gitContext(ctx.repo)} diff ${ctx.diffRange} -o geojson --output "${fileURLToPath(geojsonDiffLocation)}"`;
    const stat = await fsa.head(geojsonDiffLocation);
    const featureChangesPerDataset: Record<string, string> = {};

    if (stat && stat.isDirectory) {
      const files = await fsa.toArray(fsa.list(geojsonDiffLocation, { recursive: true }));
      for (const file of files) {
        const jsonFile = await readGeojsonFile(file);
        if (jsonFile) {
          featureChangesPerDataset[jsonFile.datasetName] = jsonFile.fileString;
        }
      }
    } else if (stat) {
      const jsonFile = await readGeojsonFile(geojsonDiffLocation);
      if (jsonFile) {
        featureChangesPerDataset[jsonFile.datasetName] = jsonFile.fileString;
      }
    }
    return featureChangesPerDataset;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:GeoJSON diff failed');
    throw error;
  }
}

async function getGitDiff(ctx: GitContext): Promise<string> {
  try {
    const tempIndexPath = fileURLToPath(new URL('.kart/no.index', ctx.repo));
    const indexPath = fileURLToPath(new URL('.kart/index', ctx.repo));
    await $`mv ${indexPath} ${tempIndexPath}`;
    const gitDiffOutput = await $`git ${gitContext(ctx.repo)} diff --no-color ${ctx.diffRange}`;
    await $`mv ${tempIndexPath} ${indexPath}`;

    await fsa.write(new URL('git_diff.txt', ctx.output), gitDiffOutput.stdout);
    return gitDiffOutput.stdout;
  } catch (error) {
    logger.error({ error, ...ctx }, 'Diff:GitDiffFailed');
    throw error;
  }
}

async function readFileWithRetry(filePath: URL, retries = 5, delay = 10): Promise<Buffer> {
  for (let i = 0; i < retries; i++) {
    const stat = await fsa.head(filePath);
    if (stat?.size) return fsa.read(filePath);
    await new Promise((resolve) => setTimeout(resolve, delay * Math.pow(2, i)));
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
      const featureCount = await getFeatureCount(ctx);
      const textDiff = await getTextDiff(ctx);
      await createHtmlDiff(ctx);

      const featureChangesPerDataset = await createGeojsonDiff(ctx);

      const gitDiff = await getGitDiff(ctx);

      const summaryMd = buildMarkdownSummary(featureCount, textDiff, gitDiff, featureChangesPerDataset);
      await fsa.write(args.summaryFile, summaryMd);
      logger.info('Diff:CommandCompleted');
    } catch (error) {
      logger.error({ error, ...ctx }, 'Diff:Failed');
      throw error;
    }
  },
});

function buildMarkdownSummary(
  featureCount: number,
  textDiff: string,
  gitDiff: string,
  featureChangesPerDataset: Record<string, string>,
): string {
  const allFeatures = Object.values(featureChangesPerDataset)
    .map((geojsonStr) => {
      const geojson = JSON.parse(geojsonStr) as GeoJson;
      return geojson.features;
    })
    .flat();

  const allChangesGeoJson = JSON.stringify({ type: 'FeatureCollection', features: allFeatures }, null, 2);
  const gitDiffLines = gitDiff.split('\n').length;

  let summary = `# Changes Summary\n\n`;
  summary += `**Total Features Changed**: ${featureCount}\n`;
  summary += `**Datasets Affected**: ${Object.keys(featureChangesPerDataset).length}\n`;
  summary += `**Git Diff Lines**: ${gitDiffLines}\n\n`;

  // Only include GeoJSON if we have features, it's not too large, and under character limit
  if (allFeatures.length > 0) {
    summary += `## Feature Changes Preview\n`;
    const geojsonLength = allChangesGeoJson.length;
    if (geojsonLength <= MAX_GEOJSON_LENGTH) {
      summary += '```geojson\n';
      summary += `${allChangesGeoJson}\n`;
      summary += '```\n\n';
    } else {
      summary += `*GeoJSON too large to display (${geojsonLength} characters > ${MAX_GEOJSON_LENGTH} limit). `;
      summary += `Check workflow artifacts for full GeoJSON data.*\n\n`;
    }
  }

  if (Object.keys(featureChangesPerDataset).length > 0) {
    summary += `### Changes by Dataset\n\n`;
    for (const [dataset, geojsonStr] of Object.entries(featureChangesPerDataset)) {
      const geojson = JSON.parse(geojsonStr) as GeoJson;
      const featureCount = geojson.features.length;
      if (featureCount > 0) {
        summary += `<details>\n<summary>**${dataset}**: ${featureCount} features changed</summary>\n\n`;
        summary += '```geojson\n';
        summary += `${geojsonStr}\n`;
        summary += '```\n';
        summary += `</details>\n\n`;
      }
    }
    summary += '\n';
  }

  summary += `## Kart Diff\n\n`;
  summary += `<details>\n`;
  summary += `<summary>Kart Diff (${featureCount} features)</summary>\n\n`;
  summary += '```diff\n';
  summary += `${textDiff}\n`;
  summary += '```\n';
  summary += `</details>\n\n`;

  summary += `## Git Diff\n\n`;
  summary += `<details>\n`;
  summary += `<summary>Git Diff (${gitDiffLines} lines)</summary>\n\n`;
  summary += '```diff\n';
  summary += `${gitDiff}\n`;
  summary += '```\n';
  summary += `</details>\n\n`;

  return summary;
}
