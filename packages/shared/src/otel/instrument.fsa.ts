import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';

import { monitor } from './instrument.ts';

function monitorName(loc: URL) {
  return `${loc.protocol.slice(0, -1)}.${loc.hostname}`;
}

export function instrumentFsa() {
  monitor(fsa, 'head', { name: (loc) => `fsa.head.${monitorName(loc)}` });
  monitor(fsa, 'read', {
    name: (loc) => `fsa.read.${monitorName(loc)}`,
    after: (_instance, span, value) => {
      span.setAttribute('file.size', value.byteLength);
      return value;
    },
  });
  monitor(fsa, 'write', {
    name: (loc) => `fsa.write.${monitorName(loc)}`,
    after: (_instance, span, value, _loc, buf) => {
      if (typeof buf === 'string') {
        span.setAttribute('file.size', buf.length);
      } else if ('byteLength' in buf) {
        span.setAttribute('file.size', buf.byteLength);
      } else if (buf instanceof HashTransform) {
        buf.on('end', () => {
          span.setAttribute('file.size', buf.bytesRead);
          span.setAttribute('file.hash', buf.multihash);
        });
      }

      return value;
    },
  });
}
