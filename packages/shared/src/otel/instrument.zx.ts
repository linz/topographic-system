import { ProcessPromise } from 'zx';

import { monitor } from './instrument.ts';

export function instrumentZx() {
  monitor(ProcessPromise.prototype, 'run', {
    name: 'zx.$',
    async before(instance, span) {
      span.updateName(`zx.$.${instance.cmd.slice(0, instance.cmd.indexOf(' '))}`);
      span.setAttribute('script.command', instance.cmd);
    },
    after(_instance, span, value) {
      span.setAttribute('script.exit', value.exitCode ?? 0);
      return value;
    },
  });
}
