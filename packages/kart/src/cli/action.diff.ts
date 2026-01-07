import { UUID } from 'node:crypto';

import { fsa } from '@chunkd/fs';
import { logger } from '@topographic-system/shared/src/log.ts';
import { command, restPositionals, string } from 'cmd-ts';
import { basename } from 'path';
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

async function getTextDiff(diffRange: string[]): Promise<string> {
  try {
    const textDiff = await $`kart -C repo diff -o "text" ${diffRange}`;
    await fsa.write(fsa.toUrl('diff/kart_diff.txt'), textDiff.stdout);
    logger.info({ textDiffLines: textDiff.stdout.split('\n').length }, 'Diff:Text diff written to diff/kart_diff.txt');
    return textDiff.stdout;
  } catch (error) {
    logger.error({ error, diffRange }, 'Diff:Text diff failed');
    throw error;
  }
}

async function getFeatureCount(diffRange: string[]): Promise<number> {
  try {
    const countOutput = await $`kart -C repo diff -o "json" ${diffRange}`;
    const featureCount = countOutput.stdout.trim();
    if (!featureCount) {
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
      if (dataset === undefined) continue;
      for (const datasetKey of Object.keys(dataset)) {
        if (dataset[datasetKey] === undefined) continue;
        totalFeaturesChanged += dataset[datasetKey]?.feature?.length ?? 0;
      }
    }
    logger.info({ diffRange, totalFeaturesChanged }, 'Diff:FeatureCount');
    return totalFeaturesChanged;
  } catch (error) {
    logger.error({ error, diffRange }, 'Diff:Feature count failed');
    throw error;
  }
}

async function createHtmlDiff(diffRange: string[]): Promise<URL> {
  try {
    const htmlFile = 'diff/kart_diff.html';
    const htmlPath = fsa.toUrl(htmlFile);
    await $`kart -C repo diff ${diffRange} -o "html" --output "${htmlFile}"`;
    const content = await readFileWithRetry(htmlPath);
    const fixedContent = content.toString('utf-8').replace(/\\x2f/g, '/').replace(/\\x3c/g, '<').replace(/\\x3e/g, '>');
    await fsa.write(htmlPath, fixedContent);
    return htmlPath;
  } catch (error) {
    logger.error({ error, diffRange }, 'Diff:HTML diff failed');
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

async function createGeojsonDiff(diffRange: string[]): Promise<Record<string, string>> {
  try {
    const geojsonOutName = 'diff/kart_diff.geojson/';
    const geojsonPath = fsa.toUrl(geojsonOutName);
    await $`kart -C repo diff ${diffRange} -o "geojson" --output "${geojsonOutName}"`;
    const stat = await fsa.head(geojsonPath);
    const featureChangesPerDataset: Record<string, string> = {};

    if (stat && stat.isDirectory) {
      const files = await fsa.toArray(fsa.list(geojsonPath, { recursive: true }));
      for (const file of files) {
        const jsonFile = await readGeojsonFile(file);
        if (jsonFile) {
          featureChangesPerDataset[jsonFile.datasetName] = jsonFile.fileString;
        }
      }
    } else if (stat) {
      const jsonFile = await readGeojsonFile(geojsonPath);
      if (jsonFile) {
        featureChangesPerDataset[jsonFile.datasetName] = jsonFile.fileString;
      }
    }
    return featureChangesPerDataset;
  } catch (error) {
    logger.error({ error, diffRange }, 'Diff:GeoJSON diff failed');
    throw error;
  }
}

async function getGitDiff(diffRange: string[]): Promise<string> {
  try {
    const gitDiffOutput =
      await $`mv repo/.kart/index repo/.kart/no.index && git --no-pager -C repo diff --no-color "${diffRange}" && mv repo/.kart/no.index repo/.kart/index`;
    await fsa.write(fsa.toUrl('diff/git_diff.txt'), gitDiffOutput.stdout);
    return gitDiffOutput.stdout;
  } catch (error) {
    logger.error({ error, diffRange }, 'Diff:Git diff failed');
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

export const diffCommand = command({
  name: 'diff',
  description: 'Run specified diff commands on a cloned kart repository',
  args: {
    diff: restPositionals({
      description: 'Commit SHA or branches to diff (default: master..FETCH_HEAD)',
      type: string,
    }),
  },
  async handler(args) {
    delete $.env['GITHUB_ACTION_REPOSITORY'];
    delete $.env['GITHUB_ACTION_REF'];
    delete $.env['GITHUB_WORKFLOW_REF'];

    logger.info({ ref: args.diff }, 'Diff:Start');

    // Determine diff range - for GitHub Actions PRs, use a more robust approach
    let diffRange: string[];
    if (args.diff.length > 0) {
      diffRange = args.diff;
    } else {
      // In GitHub Actions PR context, use merge-base to find the common ancestor
      // and compare against that rather than relying on .. operator
      try {
        // First try to find the merge base between master and FETCH_HEAD
        const mergeBase = await $`git -C repo merge-base origin/master FETCH_HEAD`;
        const baseCommit = mergeBase.stdout.trim();
        diffRange = [`${baseCommit}..FETCH_HEAD`];
        logger.info({ baseCommit, diffRange }, 'Diff:Using merge-base strategy');
      } catch (mergeBaseError) {
        // If merge-base fails, fall back to comparing against origin/master directly
        logger.warn({ error: mergeBaseError }, 'Diff:Merge-base failed, falling back to direct comparison');
        diffRange = ['origin/master', 'FETCH_HEAD'];
      }
    }

    logger.info({ diffRange }, 'Diff:Using range');

    try {
      const featureCount = await getFeatureCount(diffRange);
      const textDiff = await getTextDiff(diffRange);
      await createHtmlDiff(diffRange);

      const featureChangesPerDataset = await createGeojsonDiff(diffRange);

      const gitDiff = await getGitDiff(diffRange);

      const summaryMd = buildMarkdownSummary(featureCount, textDiff, gitDiff, featureChangesPerDataset);
      await fsa.write(fsa.toUrl('pr_summary.md'), summaryMd);
      logger.info('Diff:Markdown Summary Generated');

      logger.info('Diff command completed');
    } catch (error) {
      logger.error({ error, diffRange }, 'Diff:Failed');
      process.exit(1);
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

  let summary = `# Changes\n`;
  summary += '```geojson\n';
  summary += `${allChangesGeoJson}\n`;
  summary += '```\n\n';

  if (Object.keys(featureChangesPerDataset).length > 0) {
    summary += `### Feature Changes by Dataset\n\n`;
    summary += `<details>\n<summary>Individual GeoJSON Diffs - click each to expand</summary>\n\n`;
    for (const [dataset, geojsonStr] of Object.entries(featureChangesPerDataset)) {
      const geojson = JSON.parse(geojsonStr) as GeoJson;
      const featureCount = geojson.features.length;
      if (featureCount > 0) {
        summary += `<details>\n<summary>**${dataset}**: ${featureCount} changes</summary>\n\n`;
        summary += '```geojson\n';
        summary += `${geojsonStr}\n`;
        summary += '```\n';
        summary += `</details>\n\n`;
      }
    }
    summary += `</details>\n\n`;
  }

  summary += `## Kart Diff Results\n`;
  summary += `<details>\n`;
  summary += `<summary>kart diff - click to expand (${featureCount} features)</summary>\n\n`;
  summary += '```diff\n';
  summary += `${textDiff}\n`;
  summary += '```\n';
  summary += `</details>\n\n`;

  summary += `## Git Diff Results\n\n`;
  summary += `<details>\n`;
  summary += `<summary>git diff - click to expand (${gitDiffLines} lines)</summary>\n\n`;
  summary += '```diff\n';
  summary += `${gitDiff}\n`;
  summary += '```\n';
  summary += `</details>\n\n`;

  return summary;
}
