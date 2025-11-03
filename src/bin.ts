import { run } from 'cmd-ts';

import { Cli } from './index';

run(Cli, process.argv.slice(2)).catch((err) => {
  console.log(err);
});
