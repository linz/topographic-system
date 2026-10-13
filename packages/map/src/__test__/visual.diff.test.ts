import assert from 'node:assert';
import { afterEach, before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { StacPushCommand } from '@linzjs/topographic-system-stac';

import { DeployCommand } from '../cli/action.deploy.ts';
import { VisualDiffCommand } from '../cli/action.visual.diff.ts';
import { pyRunner } from '../python.runner.ts';
import { writeBaseLayers } from './util.ts';

describe('action.visual.diff', () => {
  const mem = new FsMemory();
  const concurrency = 10;

  before(() => {
    fsa.register('memory://', mem);
  });

  afterEach(() => {
    mem.files.clear();
  });

  it('should error if there are no resolutions used as part of the visual diff', async (t) => {
    const rootCatalog = new URL('memory://source/catalog.json');
    await writeBaseLayers(rootCatalog);

    const targetDeploy = new URL('memory://target/deploy/');
    await DeployCommand.handler({
      concurrency,
      extras: [],
      project: [new URL('memory://source/topo50maps/topo50.qgs')],
      target: targetDeploy,
      source: rootCatalog,
    });

    const targetPush = new URL('memory://target/push/');
    await StacPushCommand.handler({
      concurrency,
      source: new URL('memory://target/deploy/catalog.json'),
      target: targetPush,
      category: 'qgis',
      strategies: [{ type: 'latest' }],
      commit: true,
    });

    const projectUrl = new URL('memory://target/push/qgis/topo50/latest/topo50.json');

    const testFileUrl = new URL('memory://test-file.json');
    await fsa.write(
      testFileUrl,
      JSON.stringify([{ name: 'topo50', layout: 'tiff-50', sheetCodes: ['BQ31'], dpi: 100 }]),
    );

    const outputUrl = new URL('memory://target/visual-diff/');
    const cacheUrl = new URL('memory://temp-cache/');
    const tempLocationUrl = new URL('memory://temp-location/');

    t.mock.method(pyRunner, 'qgisExport', async (_input: URL, output: URL, sheetCode: string) => {
      const outputFile = new URL(`${sheetCode}.png`, output);
      await fsa.write(outputFile, 'fake-image-data');
      return outputFile;
    });

    await assert.rejects(
      VisualDiffCommand.handler({
        project: projectUrl,
        strategy: 'commit=abc123',
        output: outputUrl,
        cache: cacheUrl,
        tempLocation: tempLocationUrl,
        testFile: testFileUrl,
        worker: 1,
      }),
      (err: Error) => {
        assert.ok(err.message.includes('No resolutions found: strategy:commit=abc123'));
        return true;
      },
    );
  });
});
