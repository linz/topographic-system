import * as ulid from 'ulid';

/** ISO date of when this command was run */
export const CliDate = new Date().toISOString();

/** Unique Id for this instance of the cli being run */
export const CliId = ulid.ulid();

export const CliInfo = {
  package: '@topographic-system/map',
  // Git version information
  version: process.env['GIT_VERSION'],
  // Git commit hash
  hash: process.env['GIT_HASH'],
  // Github action that the CLI was built from
  buildId: process.env['GITHUB_RUN_ID'] ? `${process.env['GITHUB_RUN_ID']}-${process.env['GITHUB_RUN_ATTEMPT']}` : '',
};
