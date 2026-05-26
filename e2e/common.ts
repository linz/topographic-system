import process from 'node:process';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { $, os, ProcessOutput } from 'zx';

export const kartContainer = process.argv.find((f) => f.startsWith('--container-kart='))?.split('=')[1] ?? 'ts-kart';
export const mapContainer = process.argv.find((f) => f.startsWith('--container-map='))?.split('=')[1] ?? 'ts-map';

export const targetFolder = fsa.toUrl(`./target/`);
export const sourceAssets = fileURLToPath(new URL('./assets/', import.meta.url));
export const sourceCodeUrl = new URL('../', import.meta.url);
export const sourceCode = fileURLToPath(sourceCodeUrl);

const userGroup = os.userInfo();

export const isVerbose = process.argv.includes('--verbose');
async function runContainer(opts: { containerName: string; options?: string[] }, ...args: (string[] | string)[]) {
  for (const arg of args) console.log(`\t${Array.isArray(arg) ? arg.join('=') : arg}`);
  try {
    const ret = await $`docker run \
    --rm \
    ${opts.options ?? []} \
    -u ${userGroup.uid}:${userGroup.gid} \
    -v ${fileURLToPath(targetFolder)}:/target \
    -v ${sourceAssets}:/assets \
    -v ${sourceCode}:/source \
    ${opts.containerName} \
    ${args.flat()}`;

    if (isVerbose || ret.exitCode !== 0) console.log(`\t${ret.stdout}`);
    if (ret.exitCode !== 0) throw new Error(`Failed: ${opts.containerName}`);
    return ret;
  } catch (e) {
    if (e instanceof ProcessOutput) console.log(e.stdout);
    throw e;
  }
}

export const tsKart = runContainer.bind(null, { containerName: kartContainer });
export const tsMap = runContainer.bind(null, { containerName: mapContainer });
export const tsArgo = runContainer.bind(null, { containerName: 'ghcr.io/linz/argo-tasks:latest' });

export const tsKartImport = (...args: (string | string[])[]) =>
  runContainer(
    {
      containerName: kartContainer,
      options: [
        ['-e', 'KART_IMPORT_THEME=airport'],
        ['-e', 'KART_IMPORT_RELEASE=30,31,32'],
        ['-e', 'DAGSTER_HOME=/source/packages/kart-import/'],
        ['-e', 'UV_CACHE_DIR=/source/.uv'],
        '--entrypoint=/bin/sh',
        '--workdir=/source/packages/kart-import/',
      ].flat(),
    },
    '-c',
    args.flat().join(' '),
  );

export const skipIfExists = async (url: URL) => {
  const exists = await fsa.exists(url);
  if (exists) return { skip: true };
  return {};
};
