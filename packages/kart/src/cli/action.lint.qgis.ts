import { fsa } from '@chunkd/fs';
import { logger, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';
import { XMLParser } from 'fast-xml-parser';

export const LintQgisProjectArgs = {
  qgis: option({
    type: Url,
    long: 'qgis',
    description: 'Path to QGIS project file',
  }),
};

export const LintQgisProjectCommand = command({
  name: 'lint-qgis',
  description: 'Lint QGIS Project',
  args: LintQgisProjectArgs,
  async handler(args) {
    registerFileSystem();

    logger.info({ args }, 'LintQgis:Start');

    const qgisFile = await fsa.read(args.qgis);
    const parser = new XMLParser();
    const qgisXml = parser.parse(qgisFile);
    const errors = lintDatasources(qgisXml);

    if (errors.length > 0) {
      for (const error of errors) logger.error({ error }, 'LintQgis:Error');
      throw new Error(`QGIS project lint failed with ${errors.length} error(s):\n${errors.join('\n')}`);
    }

    logger.info('LintQgis:Completed');
  },
});

export function lintDatasources(node: unknown, visited = new Set<unknown>()): string[] {
  if (node == null || typeof node !== 'object' || visited.has(node)) {
    return [];
  }
  visited.add(node);

  const errors: string[] = [];

  for (const [key, value] of Object.entries(node)) {
    if (key === 'datasource') {
      if (typeof value !== 'string') {
        errors.push('datasource value is not a string');
        continue;
      }

      // ignore empty datasource
      if (value === '') {
        continue;
      }

      // remove datasource metadata if there, eg ./testline.parquet|layername=testline
      const path = value.split('|')[0] ?? value;
      if (!(path.startsWith('./') || path.startsWith('../'))) {
        errors.push(`datasource path must be relative (start with ./ or ../): ${value}`);
      }
    }

    errors.push(...lintDatasources(value, visited));
  }

  return errors;
}
