import { tmpdir } from 'node:os';
import path from 'node:path';

import { CliId, stringToUrlFolder, UrlFolder } from '@linzjs/topographic-system-shared';
import { option } from 'cmd-ts';

export const tempLocation = option({
  type: UrlFolder,
  long: 'temp-location',
  description: 'Where temporary files are stored, generally in /tmp/...',
  defaultValue: () => stringToUrlFolder(path.join(tmpdir(), `topo-system-${CliId}`)),
});

export const cache = option({
  type: UrlFolder,
  long: 'cache',
  description: 'Where cache files are stored, generally in /tmp/.cache',
  defaultValue: () => stringToUrlFolder(path.join(tmpdir(), `.cache/`)),
});
