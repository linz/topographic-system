import { spawn } from 'child_process';
import { command } from 'cmd-ts';

export const ProduceCommand = command({
  name: 'produce',
  description: 'Produce',
  args: {
  },

  async handler() {
    const projectPath = "/app/data/topo50-map.qgz";
    const xMin = "1732000.0";
    const yMin = "5405051.55";
    const xMax = "1756007.23";
    const yMax = "5440726.20";
    const dpi = "300";
    const fileOutputPath = "/app/data/output.pdf";

    const child = spawn('python3', ['qgis_export.py', projectPath, xMin, yMin, xMax, yMax, dpi, fileOutputPath], {
      cwd: process.cwd(),
    });
    child.stdout.on('data', (data) => console.log(`stdout: ${data}`));
    child.stderr.on('data', (data) => console.log(`stderr: ${data}`));
  },
});