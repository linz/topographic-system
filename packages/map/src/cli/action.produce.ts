import { spawn } from 'child_process';
import { command } from 'cmd-ts';

export const ProduceCommand = command({
  name: 'produce',
  description: 'Produce',
  args: {},

  handler() {
    const projectPath = '/data/topo50-map.qgz';
    const fileOutputPath = '/out';

    const child = spawn('python3', ['src/qgis_export.py', projectPath, fileOutputPath], {
      cwd: process.cwd(),
    });
    child.stdout.on('data', (data) => console.log(`stdout: ${data}`));
    child.stderr.on('data', (data) => console.log(`stderr: ${data}`));
  },
});
