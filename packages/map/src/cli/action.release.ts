import { fsa } from '@chunkd/fs';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, option } from 'cmd-ts';
import { basename } from 'path';

import { downloadFiles } from './action.download.ts';

export const ReleaseArgs = {
  source: option({
    type: UrlFolder,
    long: 'source',
    description: 'Path or s3 of source release data.',
  }),
  output: option({
    type: UrlFolder,
    long: 'output',
    description: 'Path or s3 of the output directory for release data.',
  }),
};

export const ReleaseCommand = command({
  name: 'release',
  description: 'Release',
  args: ReleaseArgs,
  async handler(args) {
    registerFileSystem();
    const sourceFiles = await downloadFiles(args.source);

    for (const file of sourceFiles) {
      const destPath = new URL(basename(file.pathname), args.output);
      const stream = fsa.readStream(file);
      await fsa.write(destPath, stream);
      logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');
    }
  },
});
