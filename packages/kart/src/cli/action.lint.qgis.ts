import { fsa } from '@chunkd/fs';
import { logger, registerFileSystem, Url } from '@linzjs/topographic-system-shared';
import { command, option } from 'cmd-ts';
import { XMLParser } from 'fast-xml-parser';

export const LintQgisProjectArgs = {
  qgis: option({
    type: Url,
    long: 'qgis',
    description: 'Path Qgis project',
  }),
};

export const LintQgisProjectCommand = command({
  name: 'lint-qgis',
  description: 'Lint Qgis Project',
  args: LintQgisProjectArgs,
  async handler(args) {
    registerFileSystem();

    logger.info({ args }, 'Lint Qgis Project: Started');

    const qgisFile = await fsa.read(args.qgis);
    const parser = new XMLParser();
    const qgisXml = parser.parse(qgisFile);
    lintDatasources(qgisXml);
  },
});

function lintDatasources(node: any, visited = new Set<any>()) {
  if (visited.has(node)) {
    return;
  }
  visited.add(node);

  for (const [key, value] of Object.entries(node)) {
    if (key === 'datasource') {
      if (typeof value !== 'string') {
        throw new Error('datasource value not a string');
      }

      if (value.endsWith('.parquet') && !value.startsWith('./')) {
        throw new Error(`datasource path must be ./something.parquet, ${value}`);
      }
    }

    lintDatasources(value, visited);
  }
}
