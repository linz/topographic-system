import { setTimeout } from 'node:timers/promises';

import { S3Client } from '@aws-sdk/client-s3';
import { fsa, FsHttp } from '@chunkd/fs';
import type { AwsCredentialConfig } from '@chunkd/fs-aws';
import { AwsS3CredentialProvider, FsAwsS3 } from '@chunkd/fs-aws';
import type { BuildMiddleware, FinalizeRequestMiddleware, MetadataBearer } from '@smithy/types';

import { logger } from './log.ts';

/** Check to see if hostname exists inside of a object */
function hasHostName(x: unknown): x is { hostname: string } {
  if (x == null) return false;
  if (typeof x === 'object' && 'hostname' in x && typeof x.hostname === 'string') return true;
  return false;
}

/**
 * AWS SDK middleware function to force fully qualified domain name  on s3 requests
 *
 * AWS S3 inside of kubernetes triggers a lot of DNS requests
 * by forcing a fully qualified domain name lookup (trailing ".")
 * it greatly reduces the number of DNS requests we make
 */
export const fqdn: FinalizeRequestMiddleware<object, MetadataBearer> = (next) => {
  return (args) => {
    if (hasHostName(args.request) && args.request.hostname.endsWith('.s3.ap-southeast-2.amazonaws.com')) {
      args.request.hostname += '.';
    }
    return next(args);
  };
};

/**
 * AWS SDK middleware logic to try 3 times if receiving an EAI_AGAIN error
 */
export function eaiAgainBuilder(timeout: (attempt: number) => number): BuildMiddleware<object, MetadataBearer> {
  const eaiAgain: BuildMiddleware<object, MetadataBearer> = (next) => {
    const maxTries = 3;
    let totalDelay = 0;
    return async (args) => {
      for (let attempt = 1; attempt <= maxTries; attempt++) {
        try {
          return await next(args);
        } catch (error) {
          if (error && typeof error === 'object' && 'code' in error && 'hostname' in error) {
            if (error.code !== 'EAI_AGAIN') {
              throw error;
            }
            const delay = timeout(attempt);
            totalDelay += delay;
            logger.warn({ host: error.hostname, attempt, delay, totalDelay }, `eai_again:retry`);
            await setTimeout(timeout(attempt));
          } else {
            throw error;
          }
        }
      }
      throw new Error(`EAI_AGAIN maximum tries (${maxTries}) exceeded`);
    };
  };
  return eaiAgain;
}

/**
 * When a new AWS file system is created copy across the middleware, but only if the middleware does not exist
 *
 * @param fileSystem to setup
 */
export function setupS3FileSystem(fileSystem: FsAwsS3): FsAwsS3 {
  const credentials = new AwsS3CredentialProvider();
  credentials.onFileSystemFound = (acc: AwsCredentialConfig, fs?: FsAwsS3, location?: URL): void => {
    if (fs == null) return;
    logger.info(
      { prefix: acc.prefix, roleArn: acc.roleArn, path: location ? location : undefined },
      'FileSystem:Register',
    );
    addMiddlewareToS3Client(fs.s3);
    fsa.register(acc.prefix, fs);
  };
  fileSystem.s3 = addMiddlewareToS3Client(fileSystem.s3);
  fileSystem.credentials = credentials;

  return fileSystem;
}

/**
 * ensure the FQDN and EAI_AGAIN Middleware exist on a s3 client
 *
 * @param s3Client to setup
 */
export function addMiddlewareToS3Client(s3Client: S3Client): S3Client {
  // There doesn't appear to be a has or find, so the only option is to list all middleware
  // which returns a list in a format: "FQDN - finalizeRequest"
  if (s3Client == null) return s3Client;
  const middleware = s3Client.middlewareStack.identify();

  if (middleware.find((f) => f.startsWith('FQDN ')) == null) {
    s3Client.middlewareStack.add(fqdn, { name: 'FQDN', step: 'finalizeRequest' });
  }

  if (middleware.find((f) => f.startsWith('EAI_AGAIN ')) == null) {
    s3Client.middlewareStack.add(
      eaiAgainBuilder((attempt: number) => 100 + attempt * 1000),
      { name: 'EAI_AGAIN', step: 'build' },
    );
  }
  return s3Client;
}

FsAwsS3.MaxListCount = 5000;

/** Split a config string into an array of strings */
export function splitConfig(x: string): string[] {
  if (x.startsWith('[')) return JSON.parse(x) as string[];
  return x
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '');
}

/** Register the S3 file system with chunkd/fsa */
export function registerFileSystem(): FsAwsS3 {
  const s3Fs = setupS3FileSystem(new FsAwsS3(new S3Client()));
  fsa.register('https://', new FsHttp());
  fsa.register('http://', new FsHttp());
  fsa.register('s3://', s3Fs);

  const configPath = process.env['AWS_ROLE_CONFIG_PATH'];
  if (configPath == null || configPath === '') return s3Fs;

  const paths = splitConfig(configPath);

  for (const path of paths) s3Fs.credentials?.registerConfig(fsa.toUrl(path), fsa as unknown as FsAwsS3);

  return s3Fs;
}

export { fsa };
