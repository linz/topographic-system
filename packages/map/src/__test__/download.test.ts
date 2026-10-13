import assert from 'node:assert';
import { afterEach, before, describe, it } from 'node:test';

import { fsa, FsMemory } from '@chunkd/fs';
import { StacPushCommand } from '@linzjs/topographic-system-stac';

import { DeployCommand } from '../cli/action.deploy.ts';
import { DownloadCommand } from '../cli/action.download.ts';
import { writeBaseLayers } from './util.ts';

describe('action.download', () => {
  const mem = new FsMemory();
  const concurrency = 10;

  before(() => {
    fsa.register('memory://', mem);
  });

  afterEach(() => {
    mem.files.clear();
  });

  it('should download project and parquet datasets into output folder', async () => {
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
    const downloadOutput = new URL('memory://target/download/');
    const cacheUrl = new URL('memory://temp-cache/');

    await DownloadCommand.handler({
      concurrency,
      linkMode: 'link-relative',
      project: projectUrl,
      output: downloadOutput,
      cache: cacheUrl,
    });

    const downloadedFiles = [...(await fsa.toArray(fsa.list(downloadOutput)))]
      .map((f) => f.href.replace(downloadOutput.href, ''))
      .sort();

    assert.deepEqual(
      downloadedFiles,
      [
        'topo50.qgs',
        'road_line.parquet',
        'water.parquet',
        'nztopo50_map_sheet.parquet',
        'nztopo50_carto_text.parquet',
      ].sort(),
    );
  });
});
