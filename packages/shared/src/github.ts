import { fsa } from '@chunkd/fs';
import { Octokit } from '@octokit/core';
import type { Api } from '@octokit/plugin-rest-endpoint-methods';
import { restEndpointMethods } from '@octokit/plugin-rest-endpoint-methods';
import { $ } from 'zx';

import { logger } from './log.ts';

export interface Blob {
  path: string;
  mode: '100644';
  type: 'blob';
  sha: string;
}

export class GithubApi {
  octokit: Api;
  repo: string;
  owner: string;

  constructor(repository: string) {
    const [owner, repo] = repository.split('/');
    if (owner == null || repo == null) throw new Error(`Badly formatted repository name: ${repository}`);
    this.owner = owner;
    this.repo = repo;

    const token = process.env['GITHUB_API_TOKEN'] ?? process.env['GITHUB_TOKEN'];
    if (token == null) throw new Error(`Please set up GITHUB_API_TOKEN or GITHUB_TOKEN environment variable.`);
    this.octokit = restEndpointMethods(new Octokit({ auth: token }));
  }

  isOk = (s: number): boolean => s >= 200 && s <= 299;
  toRef = (branch: string): string => `heads/${branch}`;

  /**
   * Create a comment on a pull request
   */
  async createComment(prNumber: number, body: string): Promise<void> {
    logger.info({ prNumber }, 'GitHub API: Create Comment');
    const response = await this.octokit.rest.issues.createComment({
      owner: this.owner,
      repo: this.repo,
      issue_number: prNumber,
      body,
    });

    if (!this.isOk(response.status)) {
      throw new Error(`Failed to create comment on PR #${prNumber}.`);
    }
    logger.info({ prNumber, url: response.data.html_url }, 'GitHub: Comment Created');
  }

  /**
   * Update the last comment made by the current user on the pull request,
   * or create a new one if none exists.
   */
  async upsertComment(prNumber: number, body: string): Promise<void> {
    logger.info({ prNumber }, 'GitHub API: Upsert Comment');
    const user = await this.octokit.rest.users.getAuthenticated();
    const login = user.data.login;

    const comments = await this.octokit.rest.issues.listComments({
      owner: this.owner,
      repo: this.repo,
      issue_number: prNumber,
      per_page: 100,
    });

    const lastComment = comments.data
      .slice()
      .reverse()
      .find((comment) => comment.user?.login === login);

    if (lastComment) {
      const response = await this.octokit.rest.issues.updateComment({
        owner: this.owner,
        repo: this.repo,
        comment_id: lastComment.id,
        body,
      });
      if (!this.isOk(response.status)) {
        throw new Error(`Failed to update comment ${lastComment.id} on PR #${prNumber}.`);
      }
      logger.info({ prNumber, commentId: lastComment.id, url: response.data.html_url }, 'GitHub: Comment Updated');
    } else {
      await this.createComment(prNumber, body);
    }
  }

  /**
   * Attempt to find the repository name (owner/repo) from the environment or git config.
   */
  static async findRepo(): Promise<string> {
    if (process.env['GITHUB_REPOSITORY']) return process.env['GITHUB_REPOSITORY'];
    logger.info('GitHub API: Finding repository from git config');
    const remoteUrl = (await $`git -C repo remote get-url origin`).stdout.trim();
    const match = remoteUrl.match(/github\.com[:/]([^/]+\/[^/.]+)(\.git)?/);
    if (!match?.[1]) throw new Error(`Could not parse GitHub repository from remote URL: ${remoteUrl}`);
    return match[1];
  }

  /**
   * Attempt to find the pull request number from the environment.
   */
  static async findPullRequest(): Promise<number | null> {
    const prNumber = process.env['GITHUB_PR_NUMBER'];
    if (prNumber) return parseInt(prNumber, 10);
    const ref = process.env['GITHUB_REF'];
    if (ref?.startsWith('refs/pull/')) {
      const match = ref.match(/refs\/pull\/(\d+)/);
      if (match?.[1]) return parseInt(match[1], 10);
    }

    const eventPath = process.env['GITHUB_EVENT_PATH'];
    if (eventPath) {
      try {
        const fileContent = await fsa.read(fsa.toUrl(eventPath));
        const event = JSON.parse(fileContent.toString('utf-8'));
        return event.pull_request?.number ?? event.number ?? null;
      } catch {
        // Fall through
      }
    }

    return null;
  }
}
