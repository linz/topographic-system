import path, { basename } from 'path';
import { cwd } from 'process';
import { fileURLToPath, pathToFileURL } from 'url';

import { fsa } from '@chunkd/fs';
import type { CommandExecution, CommandExecutionResult } from '@linzjs/docker-command';
import { Command } from '@linzjs/docker-command';
import { logger, trace } from '@linzjs/topographic-system-shared';

import { getQgisMapSheetDataset, getQgisProjectMeta } from './qgis.ts';
import type { ExportOptions } from './stac.ts';

export const BaseCommandOptions = {
  useDocker: false,
  container: 'ghcr.io/linz/qgis-flatpak:linz-qgis_70e40a-ff162c_build-43',
};

const Python3 = new Command('python3', BaseCommandOptions);

/** Location of the source files if they have been found by {@link findQgisSource} */
let sourceUrl: URL | null = null;

/**
 * The QGIS source python files can be located in a few locations
 * depending opn how the script has been deployed
 *
 * Search a few locations to try and find the "qgis_export.py" script
 *
 * @throws if it cannot find the qgis_export.py script
 */
async function findQgisSource(): Promise<URL> {
  if (sourceUrl) return sourceUrl;
  const fileSourceUrl = import.meta.url ?? pathToFileURL(__filename);
  // import.meta.url will not exist in commonjs contexts so attempt to use the CWD as a fall back
  for (const currentUrl of [fileSourceUrl, pathToFileURL(cwd() + path.sep)]) {
    if (currentUrl == null) continue;
    const sameFolder = new URL('qgis/src/qgis_export.py', currentUrl);
    const isSameFolder = await fsa.exists(sameFolder);
    if (isSameFolder === true) {
      sourceUrl = new URL('.', sameFolder);
      return sourceUrl;
    }
    logger.debug({ target: sameFolder.href }, 'Python:Source:Missing');

    const parentLocation = new URL('../../qgis/src/qgis_export.py', currentUrl);
    const isParentLocation = await fsa.exists(parentLocation);
    if (isParentLocation === true) {
      sourceUrl = new URL('.', parentLocation);
      return sourceUrl;
    }
    logger.debug({ target: parentLocation.href }, 'Python:Source:Missing');
  }

  throw new Error('Unable to find QGIS source files');
}

async function runAndLog(cmd: CommandExecution): Promise<CommandExecutionResult> {
  const script = basename(cmd.args[0] ?? 'unknown');
  return trace(`python.${script}`, async (span) => {
    span.setAttribute('script.name', script);
    span.setAttribute('script.arguments', cmd.args.slice(1));

    logger.debug({ script, args: cmd.args.slice(1) }, 'Python:Start');

    const startTime = performance.now();
    const res = await cmd.run();

    logger.info({ script, duration: performance.now() - startTime }, 'Python:Done');
    span.setAttribute('script.exit', res.exitCode);

    if (res.exitCode !== 0) {
      logger.fatal({ script, stderr: res.stderr, stdout: res.stdout }, 'Failure');
      throw new Error(`${script} failed to run`);
    }
    return res;
  });
}

/**
 * Running python commands for qgis_export
 */
async function qgisExport(input: URL, output: URL, sheetCode: string, options: ExportOptions): Promise<URL> {
  const startTime = performance.now();
  const sourceLocation = await findQgisSource();

  // We store the dataset name as nztopo50_map_sheet.parquet
  // But we need the layer name for the qgis project
  const projectMeta = await getQgisProjectMeta(input);
  const mapSheetLayerName = getQgisMapSheetDataset(projectMeta.layers, options.mapSheetDataset);
  if (mapSheetLayerName == null){
    throw new Error(`Unable to find map sheet layer for dataset: ${options.mapSheetDataset}`);
  }

  const cmd = Python3.create(BaseCommandOptions);

  cmd.mount(fileURLToPath(sourceLocation));
  cmd.mount(fileURLToPath(new URL('.', input)));
  cmd.mount(fileURLToPath(new URL('.', output)));

  cmd.args.push(fileURLToPath(new URL('qgis_export.py', sourceLocation)));
  cmd.args.push(fileURLToPath(input));
  cmd.args.push(fileURLToPath(output));
  cmd.args.push(options.layout);
  cmd.args.push(mapSheetLayerName.name);
  cmd.args.push(options.format);
  cmd.args.push(options.dpi.toFixed());
  cmd.args.push(sheetCode);
  cmd.args.push(JSON.stringify(options.excludeLayers ?? []));

  const res = await runAndLog(cmd);
  logger.info({ sheetCode, output: res.stdout.trim(), duration: performance.now() - startTime }, 'Export:Done');
  return pathToFileURL(res.stdout.trim());
}

/**
 * Load and print the QGIS verison from python
 *
 * @example "4.0.0-Norrköping"
 *
 * @returns Qgis version from python
 */
async function qgisVersion(): Promise<string> {
  const sourceLocation = await findQgisSource();
  const cmd = Python3.create(BaseCommandOptions);
  cmd.args.push(fileURLToPath(new URL('qgis_version.py', sourceLocation)));

  const res = await runAndLog(cmd);

  return res.stdout.trim();
}

/** Redefined for testing */
export const pyRunner = { qgisExport, qgisVersion };
