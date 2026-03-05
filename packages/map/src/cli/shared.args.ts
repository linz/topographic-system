import { tmpdir } from 'node:os';
import path from 'node:path';

import { UrlFolder, CliId } from '@linzjs/topographic-system-shared';
import { stringToUrlFolder } from '@linzjs/topographic-system-shared/src/url.ts';
import { option } from 'cmd-ts';

export const tempLocation = option({
  type: UrlFolder,
  long: 'temp-location',
  description: 'Where temporary files are stored, generally in /tmp/...',
  defaultValue: () => stringToUrlFolder(path.join(tmpdir(), `topo-system-${CliId}`)),
});
