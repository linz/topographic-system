import { fsa } from '@chunkd/fs';
import { spawn } from 'child_process';
import { command, option, optional, restPositionals, string } from 'cmd-ts';
import { registerFileSystem } from '../fs.register.ts';

/** Ready the json file and parse all the mapsheet code as array */
async function fromFile(file: URL): Promise<string[]> {
  const mapSheets = await fsa.readJson<string[]>(file);
  if (mapSheets == null || mapSheets.length == 0) throw new Error(`Invalide or empty map sheets in file: ${file.href}`);
  return mapSheets;
}

export const ProduceArgs = {
  mapSheet: restPositionals({ type: string, displayName: 'map-sheet', description: 'Map Sheet Code to process' }),
  fromFile: option({
    type: optional(string),
    long: 'from-file',
    description: 'Path to JSON file containing array of MapSheet Codes to Process.',
  }),
  source: option({
    type: string,
    long: 'source',
    description: 'Path or s3 of QGIS Project to use for generate map sheets.',
  }),
  project: option({
    type: string,
    long: 'project',
    description: 'Path or s3 of source parquet vector layers to use for generate map sheets.',
  }),
  output: option({
    type: string,
    long: 'output',
    description: 'Path or s3 of the output directory to write generated map sheets.',
  }),
};

export const ProduceCommand = command({
  name: 'produce',
  description: 'Produce',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();
    const source = new URL(args.source);
    const project = new URL(args.project);
    const output = new URL(args.output);
    const file = args.fromFile;

    // Prepare all the map sheets to process
    const mapSheets = file != null ? args.mapSheet.concat(await fromFile(new URL(file))) : args.mapSheet;

    const child = spawn('python3', ['src/python/qgis_export.py', projectPath, fileOutputPath], {
      cwd: process.cwd(),
    });
    child.stdout.on('data', (data) => console.log(`stdout: ${data}`));
    child.stderr.on('data', (data) => console.log(`stderr: ${data}`));
  },
});
