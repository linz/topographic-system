import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import { UrlFolder, CliId } from '@linzjs/topographic-system-shared';
import { option } from 'cmd-ts';

export const tempLocation = option({
  type: UrlFolder,
  long: 'temp-location',
  description: 'Where temporary files are stored, generally in /tmp/...',
  defaultValue: () => fsa.toUrl(path.join(tmpdir(), `topo-system-${CliId}`)),
});
