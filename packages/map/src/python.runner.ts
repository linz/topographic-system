import path, { basename } from 'path';
import { cwd } from 'process';
import { fileURLToPath, pathToFileURL } from 'url';

import { Bounds, Projection, ProjectionLoader } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import type { CommandExecution, CommandExecutionResult } from '@linzjs/docker-command';
import { Command } from '@linzjs/docker-command';
import { trace, logger } from '@linzjs/topographic-system-shared';
import { multipolygonToWgs84, polygonToWgs84, round } from '@linzjs/topographic-system-stac';
import type { GeoJSONMultiPolygon, GeoJSONPolygon } from 'stac-ts';

import type { ExportOptions } from './stac.ts';

export const BaseCommandOptions = {
  useDocker: false,
  container: 'ghcr.io/linz/qgis-flatpak:linz-qgis_1ddcee-a6a40d_build-34',
};

interface SheetMetadataStdOut {
  sheetCode: string;
  geometry: string; // Geometry encoded as string
  epsg: number;
  bbox: [number, number, number, number];
}

export type SheetMetadata = {
  sheetCode: string;
  geometry: GeoJSONPolygon | GeoJSONMultiPolygon;
  epsg: number;
  bbox: [number, number, number, number];
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

export async function parseSheetsMetadata(stdoutBuffer: string): Promise<SheetMetadata[]> {
  const raw = JSON.parse(stdoutBuffer) as SheetMetadataStdOut[];

  const metadata: SheetMetadata[] = [];
  for (const item of raw) {
    // FIXME: Missing some floating number like 0.25 and 0.5 and adding some floating number like 0.000000001 in the output of qgis_export_cover.py,
    // which cause the bbox to be different from the original one in qgis project and cause the stac item to be different from the original one. Need to investigate why this happens and how to fix it.
    const geom = typeof item.geometry === 'string' ? (JSON.parse(item.geometry) as GeoJSON.Geometry) : item.geometry;

    // Only could be a polygon or multipolygons for a mapsheet.
    if (geom.type !== 'Polygon' && geom.type !== 'MultiPolygon') {
      throw new Error(`Unexpected geometry type for ${item.sheetCode}: ${geom.type}`);
    }

    // Convert the geometries and bbox into wsg84
    const epsg = await ProjectionLoader.load(item.epsg);
    const proj = Projection.get(epsg);
    if (geom.type === 'Polygon') {
      geom.coordinates = polygonToWgs84(proj, geom.coordinates);
    } else if (geom.type === 'MultiPolygon') {
      geom.coordinates = multipolygonToWgs84(proj, geom.coordinates);
    }

    // geom.coordinates = truncate(geom.coordinates)
    // Convert bbox to wgs84
    const bounds = Bounds.fromBbox(item.bbox);
    const bbox = proj.boundsToWgs84BoundingBox(bounds);

    metadata.push({
      sheetCode: item.sheetCode,
      geometry: geom.type === 'Polygon' ? (geom as GeoJSONPolygon) : (geom as GeoJSONMultiPolygon),
      epsg: item.epsg,
      bbox: [round(bbox[0]), round(bbox[1]), round(bbox[2]), round(bbox[3])],
    });
  }

  return metadata;
}

/**
 * Running python commands for qgis_export
 */
async function qgisExport(input: URL, output: URL, sheetCode: string, options: ExportOptions): Promise<URL> {
  const startTime = performance.now();
  const sourceLocation = await findQgisSource();
  const cmd = Python3.create(BaseCommandOptions);

  cmd.mount(fileURLToPath(sourceLocation));
  cmd.mount(fileURLToPath(new URL('.', input)));
  cmd.mount(fileURLToPath(new URL('.', output)));

  cmd.args.push(fileURLToPath(new URL('qgis_export.py', sourceLocation)));
  cmd.args.push(fileURLToPath(input));
  cmd.args.push(fileURLToPath(output));
  cmd.args.push(options.layout);
  cmd.args.push(options.mapSheetLayer);
  cmd.args.push(options.format);
  cmd.args.push(options.dpi.toFixed());
  cmd.args.push(sheetCode);
  cmd.args.push(JSON.stringify(options.excludeLayers ?? []));

  const res = await runAndLog(cmd);
  logger.info({ sheetCode, output: res.stdout.trim(), duration: performance.now() - startTime }, 'Export:Done');
  return pathToFileURL(res.stdout.trim());
}

/**
 * Running python commands for qgis_export_cover
 * This command is used to load map sheet layers from the input project and mapsheet data and return geometry and metadata of the mapsheets.
 * @param input URL of the input QGIS project file
 * @param options ExportOptions containing layout and mapSheetLayer information
 *
 * @param mapsheets Optional to specify which mapsheets to list. If not provided, all mapsheets in the project will be listed.
 *
 * @returns mapsheet metadata including sheetcode and geometry information for the stac files
 */
export async function qgisExportCover(
  input: URL,
  options: ExportOptions,
  mapsheets?: string[],
): Promise<SheetMetadata[]> {
  const sourceLocation = await findQgisSource();
  const cmd = Python3.create(BaseCommandOptions);

  cmd.mount(fileURLToPath(sourceLocation));
  cmd.mount(fileURLToPath(new URL('.', input)));

  cmd.args.push(fileURLToPath(new URL('qgis_export_cover.py', sourceLocation)));
  cmd.args.push(fileURLToPath(input));
  cmd.args.push(options.layout);
  cmd.args.push(options.mapSheetLayer);
  // list all if mapsheets is not provided, otherwise list the mapsheets passed from CLI
  if (mapsheets) {
    cmd.args.push('False');
    for (const mapsheet of mapsheets) cmd.args.push(mapsheet);
  } else {
    cmd.args.push('True');
  }
  const res = await runAndLog(cmd);
  return await parseSheetsMetadata(res.stdout);
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

/**
 * Running python commands for list_source_layers
 */
async function listSourceLayers(input: URL): Promise<string[]> {
  // return ['testmapsheet']
  const sourceLocation = await findQgisSource();
  const cmd = Python3.create(BaseCommandOptions);
  //
  cmd.mount(fileURLToPath(sourceLocation));
  cmd.mount(fileURLToPath(new URL('.', input)));
  //
  cmd.args.push(fileURLToPath(new URL('list_source_layers.py', sourceLocation)));
  cmd.args.push(fileURLToPath(input));
  const res = await runAndLog(cmd);
  //
  // Get all layers names and remove duplicates
  const layerPaths = JSON.parse(res.stdout) as string[];
  const layerNames = Array.from(new Set(layerPaths.map((p) => path.basename(p, path.extname(p)))));
  //
  return layerNames;
}

/** Redefined for testing */
export const pyRunner = { listSourceLayers, qgisExport, qgisExportCover, qgisVersion };
