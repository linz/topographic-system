import { fileURLToPath } from 'node:url';

import { gitContext, logger, parseEnv, stringToUrlFolder, UrlFolder } from '@linzjs/topographic-system-shared';
import { command, option, optional, positional, string } from 'cmd-ts';
import { z } from 'zod/mini';
import { $ } from 'zx';

const EnvParser = z.object({
  GITHUB_TOKEN: z.optional(z.string()),
});

/** Input arguments for the `clone` command */
export interface CloneArgs {
  /**
   * Repository to clone. Accepts full URL or shorthand org/repo slug.
   * Only accepts github.com hosted repositories (others will throw at runtime).
   *
   * @example "https://github.com/linz/topographic-data"
   * @example "linz/topographic-data"
   */
  repository: string;

  /**
   * Output directory for the cloned repository. Defaults to "./repo" in the current working directory.
   *
   * @example new URL("file:///tmp/repo/")
   */
  output?: URL;

  /**
   * Commit SHA or branch to fetch after cloning. If omitted, uses the `master` branch.
   *
   * @example "master"
   */
  ref?: string;
}

/**
 * Resolved context built by {@link buildCloneContext} from {@link CloneArgs}
 * and an optional GitHub token.
 */
export interface CloneContext {
  /**
   * Local directory where the repository will be cloned. Mirrors {@link CloneArgs.output}.
   */
  target: URL;

  /**
   * Resolved `github.com` URL of the repository, from {@link CloneArgs.repository}.
   * Does not include credentials, even if a token is provided.
   * Use this for logging.
   *
   * @example new URL("https://github.com/linz/topographic-data")
   */
  repoUrl: URL;

  /**
   * Clone of {@link repoUrl} with credentials injected when a `GITHUB_TOKEN`
   * is available (`username = "x-access-token"`, `password = token`).
   * Without a token this is identical to `repoUrl`.
   * Treat as a secret. Do not log.
   *
   * @example new URL("https://x-access-token:ghp_token@github.com/linz/topographic-data")
   */
  credentialUrl: URL;

  /**
   * Resolved ref to fetch after cloning. Mirrors {@link CloneArgs.ref}.
   */
  ref: string;
}

/**
 * Build the clone context: resolve the output path, construct the repo URL,
 * and inject credentials when a GITHUB_TOKEN is available.
 */
export function buildCloneContext(args: CloneArgs, token?: string): CloneContext {
  const target = args.output ?? stringToUrlFolder('repo');
  const ref = args.ref ?? 'master';

  const repoUrl = new URL(args.repository, 'https://github.com/');

  if (repoUrl.host !== 'github.com') {
    throw new Error('Invalid host: ' + repoUrl.host);
  }

  const credentialUrl = new URL(repoUrl);
  if (token != null) {
    credentialUrl.username = 'x-access-token';
    credentialUrl.password = token;
  }

  return { target, repoUrl, credentialUrl, ref };
}

export const CloneCommand = command({
  name: 'clone',
  description: 'Clone a kart repository and fetch a specific commit',
  args: {
    repository: positional({
      displayName: 'repository',
      description: 'Repository to clone (e.g. linz/topographic-data)',
    }),
    output: positional({
      type: optional(UrlFolder),
      displayName: 'output',
      description: 'Output directory for the cloned repository (default: repo)',
    }),
    ref: option({
      type: optional(string),
      long: 'ref',
      description: 'Commit SHA or branch to fetch (default: master)',
      defaultValue: () => 'master',
    }),
  },
  async handler(args) {
    logger.info({ repository: args.repository, ref: args.ref }, 'Clone:Start');

    const env = parseEnv(EnvParser);
    const ctx = buildCloneContext(args, env.GITHUB_TOKEN);

    logger.debug({ repoUrl: ctx.repoUrl.href }, 'Clone:NoCheckout');
    await $`kart clone ${ctx.credentialUrl.href} --no-checkout ${fileURLToPath(ctx.target)}`;
    logger.debug({ repoUrl: ctx.repoUrl.href, ref: ctx.ref }, 'Clone:Completed');
    await $`kart ${gitContext(ctx.target)} fetch origin ${ctx.ref}`;
    logger.info({ repoUrl: ctx.repoUrl.href, ref: ctx.ref }, 'Fetch:Completed');
  },
});
