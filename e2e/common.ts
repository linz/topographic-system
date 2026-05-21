import process from 'node:process';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { $, os, ProcessOutput } from 'zx';

export const kartContainer = process.argv.find((f) => f.startsWith('--container-kart='))?.split('=')[1] ?? 'ts-kart';
export const mapContainer = process.argv.find((f) => f.startsWith('--container-map='))?.split('=')[1] ?? 'ts-map';

export const targetFolder = fsa.toUrl(`./target/`);
export const sourceAssets = fileURLToPath(new URL('./assets/', import.meta.url));
export const sourceCode = fileURLToPath(new URL('../', import.meta.url));

const userGroup = os.userInfo();

export const isVerbose = process.argv.includes('--verbose');
async function runContainer(containerName: string, ...args: (string[] | string)[]) {
  for (const arg of args) console.log(`\t${Array.isArray(arg) ? arg.join('=') : arg}`);

  console.log(args)
  try {
    const ret = await $`docker run \
    --rm \
    -u ${userGroup.uid}:${userGroup.gid} \
    -v ${fileURLToPath(targetFolder)}:/target \
    -v ${sourceAssets}:/assets \
    -v ${sourceCode}:/source \
    ${containerName} \
    ${args.flat()}`;

    if (isVerbose || ret.exitCode !== 0) console.log(`\t${ret.stdout}`);
    if (ret.exitCode !== 0) throw new Error(`Failed: ${containerName}`);
    return ret;
  } catch (e) {
    if (e instanceof ProcessOutput) console.log(e.stdout);
    throw e;
  }
}

export const tsKart = runContainer.bind(null, kartContainer);
export const tsMap = runContainer.bind(null, mapContainer);
export const tsArgo = runContainer.bind(null, 'ghcr.io/linz/argo-tasks:latest');

export const tsKartImport = (...args: (string | string[])[]) => runContainer("--entrypoint=/bin/sh", "--workdir=/source/packages/kart-import/", kartContainer, '-c', args.flat().join(' '));


export const skipIfExists = async (url: URL) => {
  const exists = await fsa.exists(url);
  if (exists) return { skip: true };
  return {};
}
