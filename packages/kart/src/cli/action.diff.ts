import { fsa } from '@chunkd/fs';
import { logger } from '@topographic-system/shared/src/log.ts';
import { command, restPositionals, string } from 'cmd-ts';
import { basename } from 'path';
import { $ } from 'zx';

async function getTextDiff(diffRange: string[]): Promise<string> {
  // read text diff
  // $.verbose = true  // Shows commands and their output
  // $.shell = '/bin/bash'
  // $.prefix = 'set -x; '  // Bash will echo each command
  const textDiff = await $`kart -C repo diff ${diffRange} -o "text"`;
  await fsa.write(fsa.toUrl('kart_diff.txt'), textDiff.stdout);
  logger.info({ textDiffLines: textDiff.stdout.split('\n').length }, 'Diff:Text diff written to kart_diff.txt');
  return textDiff.stdout;
}
async function getFeatureCount(diffRange: string[]): Promise<string> {
  // read feature count
  // const baseDiffCmd = `-C repo diff ${diffRange}`;
  // $.verbose = true  // Shows commands and their output
  // $.shell = '/bin/bash'
  // $.prefix = 'set -x; '  // Bash will echo each command
  // await $`pwd; ls -latr; kart -C repo diff master..origin/feat-kart-edits -- --only-feature-count exact`;
  // const countOutput = await $`pwd; whoami; id; ls -latr ./repo/; cat ./repo/.git; ls ./repo/.kart/ -altr; kart -C repo diff --only-feature-count exact ${diffRange}`;
  const countOutput = await $`kart -C repo diff --only-feature-count exact ${diffRange}`;
  const featureCount = countOutput.stdout.trim();
  logger.info({ diffRange, featureCount }, 'Diff:FeatureCount');
  return featureCount;
}

async function readFileWithRetry(filePath: URL, retries = 5, delay = 10): Promise<Buffer> {
  for (let i = 0; i < retries; i++) {
    try {
      const stat = await fsa.head(filePath);
      if (!stat) throw new Error('File not found');
      return await fsa.read(filePath);
    } catch (e) {
      logger.warn({ error: e, retry: i }, 'ReadFile:Failed');
      if (i === retries - 1) throw e;
      await new Promise((resolve) => setTimeout(resolve, delay * Math.pow(2, i)));
    }
  }
  throw new Error(`Failed to read file after ${retries} retries`);
}

async function createHtmlDiff(diffRange: string[]): Promise<URL> {
  // create HTML diff
  // const baseDiffCmd = `kart -C repo diff ${diffRange}`;
  const htmlFile = 'kart_diff.html';
  const htmlPath = fsa.toUrl(htmlFile);
  await $`kart -C repo diff ${diffRange} -o "html" --output "${htmlFile}"`;
  // await $`echo "HERE ${htmlFile}"; ls -latr .; pwd`;
  // await $`echo "href"; ls -latr ${htmlPath.href}`;
  // await $`echo "file"; ls -latr ${htmlFile}`;
  const content = await readFileWithRetry(htmlPath);
  const fixedContent = content.toString('utf-8').replace(/\\x2f/g, '/').replace(/\\x3c/g, '<').replace(/\\x3e/g, '>');
  await fsa.write(htmlPath, fixedContent);
  return htmlPath;
}

type GeoJson = { type: string; features: unknown[] };

async function readGeojsonFile(file: URL): Promise<{ datasetName: string; fileString: string } | undefined> {
  const datasetName = basename(file.href, '.geojson');
  const fileContent = await readFileWithRetry(file);
  const fileString = fileContent.toString('utf-8');
  const geojson = JSON.parse(fileString) as GeoJson;
  if (geojson.type === 'FeatureCollection' && Array.isArray(geojson.features)) {
    // allFeatures.push(...geojson.features);
    return { datasetName, fileString };
  }
  return undefined;
}

async function createGeojsonDiff(diffRange: string[]): Promise<Record<string, string>> {
  // create geojson diff
  // const baseDiffCmd = `kart -C repo diff ${diffRange}`;
  const geojsonOutName = 'kart_diff.geojson';
  const geojsonPath = fsa.toUrl(geojsonOutName);
  await $`kart -C repo diff ${diffRange} -o "geojson" --output "${geojsonOutName}"`;
  const stat = await fsa.head(geojsonPath);
  const featureChangesPerDataset: Record<string, string> = {};
  // const allFeatures: any[] = [];

  if (stat && stat.isDirectory) {
    // Combine all files in the directory into one geojson file
    const files = await fsa.toArray(fsa.list(geojsonPath, { recursive: true }));
    for (const file of files) {
      const jsonFile = await readGeojsonFile(file);
      if (jsonFile) {
        featureChangesPerDataset[jsonFile.datasetName] = jsonFile.fileString;
      }
    }
  } else if (stat) {
    // no "isFile" property on stat, so just use else
    const jsonFile = await readGeojsonFile(geojsonPath);
    if (jsonFile) {
      featureChangesPerDataset[jsonFile.datasetName] = jsonFile.fileString;
    }
  }
  return featureChangesPerDataset;
}

async function getGitDiff(diffRange: string[]): Promise<string> {
  // Get git diff
  const gitDiffOutput =
      await $`mv repo/.kart/index repo/.kart/no.index && git --no-pager -C repo diff --no-color "${diffRange}" && mv repo/.kart/no.index repo/.kart/index`;
  await fsa.write(fsa.toUrl('git_diff.txt'), gitDiffOutput.stdout);
  return gitDiffOutput.stdout;
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
    logger.info({ ref: args.diff }, 'Diff:Start');
    const diffRange = args.diff.length > 0 ? args.diff : ['master..FETCH_HEAD'];

    const featureCount = await getFeatureCount(diffRange);
    // read text diff
    const textDiff = await getTextDiff(diffRange);
    const htmlFile = await createHtmlDiff(diffRange);

    // const uploadedHtmlPath = await uploadToArtifactStorage(htmlFile);
    const uploadedHtmlPath = htmlFile.href;
    // const uploadedGeojsonPath = await uploadToArtifactStorage('kart_diff.geojson');

    const featureChangesPerDataset = await createGeojsonDiff(diffRange);

    // const allChangesGeoJson = JSON.stringify({ type: 'FeatureCollection', features: allFeatures }, null, 2);
    // await fsa.write(fsa.toUrl('kart_diff_all.geojson'), allChangesGeoJson);
    const gitDiff = await getGitDiff(diffRange);

    // build Markdown summary
    const summaryMd = buildMarkdownSummary(
        diffRange,
        featureCount,
        textDiff,
        gitDiff,
        featureChangesPerDataset,
        uploadedHtmlPath,
    );

    await fsa.write(fsa.toUrl('pr_summary.md'), summaryMd);
    logger.info('Diff:Markdown Summary Generated');

    logger.info('Diff command completed');
  },
});

function buildMarkdownSummary(
    diffRange: string[],
    featureCount: string,
    textDiff: string,
    gitDiff: string,
    featureChangesPerDataset: Record<string, string>,
    htmlArtifactUrl: string,
): string {
  const allFeatures = Object.values(featureChangesPerDataset)
      .map((geojsonStr) => {
        const geojson = JSON.parse(geojsonStr) as GeoJson;
        return geojson.features;
      })
      .flat();

  const allChangesGeoJson = JSON.stringify(
      { type: 'FeatureCollection', features: allFeatures },
      null,
      2
  );
  // const allChangesGeoJson = Object.values(featureChangesPerDataset)
  //   .map((geojsonStr) => {
  //     const geojson = JSON.parse(geojsonStr) as GeoJson;
  //     return geojson.features;
  //   })
  //   .flat()
  //   .join(',\n');
  const gitDiffLines = gitDiff.split('\n').length;

  let summary = `# Changes\n`;
  summary += `To view a map and table with all changes between \`${diffRange}\`, see the HTML file in the [attached artifacts](${htmlArtifactUrl}).\n\n`;

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
        summary += `${geojsonStr}\n`
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
