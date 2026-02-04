import { fsa } from '@chunkd/fs';
import { CliDate, CliInfo } from '@topographic-system/shared/src/cli.info.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { RootCatalogFile } from '@topographic-system/shared/src/stac.ts';
import { basename } from 'path';
import { $ } from 'zx';

export async function recursiveFileSearch(sourcePath: URL, extension?: string): Promise<URL[]> {
  const stat = await fsa.head(sourcePath);
  if (stat && stat.isDirectory) {
    const filePaths = await fsa.toArray(fsa.list(sourcePath, { recursive: true }));
    if (extension) {
      return filePaths.filter((filePath) => filePath.href.endsWith(extension));
    }
    return filePaths;
  }
  if (stat) {
    if (extension === undefined || sourcePath.href.endsWith(extension)) {
      return [sourcePath];
    }
  }
  return [];
}

export function determineAssetLocation(subdir: string, dataset: string, output: string, tag?: string): URL {
  if (!tag) {
    if (is_merge_to_master() || is_release()) {
      tag = `year=${CliDate.slice(0, 4)}/date=${CliDate}`;
    } else if (is_pr()) {
      const ref = $.env['GITHUB_REF_NAME'] || '';
      const prMatch = ref.match(/(\d+)\/merge/);
      if (prMatch) {
        tag = `pull_request/pr-${prMatch[1]}`;
      } else {
        tag = `pull_request/unknown`;
      }
    } else {
      tag = `dev-${CliInfo.hash}`;
    }
  }
  logger.info(
    { subdir, dataset, tag, master: is_merge_to_master(), release: is_release(), pr: is_pr() },
    'ToParquet:DetermineS3LocationContextVars',
  );
  return new URL(`${subdir}/${dataset}/${tag}/${basename(output)}`, RootCatalogFile);
}

export function is_pr(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return ref.startsWith('refs/pull/');
}

export function is_merge_to_master(): boolean {
  const ref = $.env['GITHUB_REF'] || '';
  return !is_pr() && ref.endsWith('/master');
}

export function is_release(): boolean {
  const workflow = $.env['GITHUB_WORKFLOW_REF'] || '';
  return is_merge_to_master() && workflow.toLowerCase().includes('release');
}
