import assert from 'node:assert';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { after, afterEach, before, describe, it, mock } from 'node:test';
import { pathToFileURL } from 'node:url';

import { stringToUrlFolder } from '@linzjs/topographic-system-shared';

import { CloneCommand } from '../action.clone.ts';
import { DiffCommand } from '../action.diff.ts';
import { ExportCommand } from '../action.export.ts';
import { FlowCommand } from '../action.flow.ts';
import { CommentCommand } from '../action.pr.comment.ts';
import { ParquetCommand } from '../action.to.parquet.ts';
import { ValidateSchemaCommand } from '../action.validate.schema.ts';
import { ValidateCommand } from '../action.validate.ts';
import { VersionCommand } from '../action.version.ts';

function mockAllHandlers(callOrder?: string[]) {
  return {
    version: mock.method(VersionCommand, 'handler', async () => {
      callOrder?.push('version');
    }),
    clone: mock.method(CloneCommand, 'handler', async () => {
      callOrder?.push('clone');
    }),
    diff: mock.method(DiffCommand, 'handler', async () => {
      callOrder?.push('diff');
    }),
    comment: mock.method(CommentCommand, 'handler', async () => {
      callOrder?.push('pr-comment');
    }),
    export: mock.method(ExportCommand, 'handler', async () => {
      callOrder?.push('export');
    }),
    parquet: mock.method(ParquetCommand, 'handler', async () => {
      callOrder?.push('to-parquet');
    }),
    validate: mock.method(ValidateCommand, 'handler', async () => {
      callOrder?.push('validate');
    }),
  };
}

describe('action.flow', () => {
  let testDir: string;
  let defaultFlowArgs: Parameters<typeof FlowCommand.handler>[0];
  const defaultEnv = { ...process.env };

  before(async () => {
    testDir = 'kart-prepare-test/';
    // Make canCommentOnPr() return true for all flow tests
    process.env['GITHUB_REF'] = 'refs/pull/123/merge';
    process.env['GITHUB_TOKEN'] = 'fake-token';
    delete process.env['GITHUB_PR_NUMBER'];
    delete process.env['GITHUB_EVENT_PATH'];
    defaultFlowArgs = {
      // Flow-level args
      worker: 2,
      concurrency: 10,
      repository: 'linz/topographic-test-data',
      ref: 'master',
      output: stringToUrlFolder(path.join(testDir, 'output')),
      changedDatasetsOnly: false,

      // Clone
      cloneOutput: stringToUrlFolder(path.join(testDir, 'repo')),

      // Diff
      diffOutput: stringToUrlFolder(path.join(testDir, 'diff')),
      summaryFile: pathToFileURL(path.join(testDir, 'pr_summary.md')),

      // Export
      exportOutput: stringToUrlFolder(path.join(testDir, 'export')),
      exportRef: 'FETCH_HEAD',

      // To-parquet
      compression: 'zstd',
      compressionLevel: 17,
      sortByBbox: true,
      rowGroupSize: 4096,
      parquetTempLocation: stringToUrlFolder(path.join(testDir, 'parquet')),

      // Validate
      schemaDirectory: stringToUrlFolder(path.join(testDir, 'schema')),
      configFile: pathToFileURL('/packages/validation/config/default_config.json'),
      validationOutputDir: stringToUrlFolder(path.join(testDir, 'validation-output')),
    };
  });

  afterEach(() => {
    mock.restoreAll();
  });

  after(async () => {
    for (const key of Object.keys(process.env)) {
      if (!(key in defaultEnv)) delete process.env[key];
    }
    Object.assign(process.env, defaultEnv);
  });

  it('should run all steps in order', async () => {
    const callOrder: string[] = [];

    const mocks = mockAllHandlers(callOrder);

    await FlowCommand.handler({ ...defaultFlowArgs, ref: 'my-branch', changedDatasetsOnly: true });

    assert.deepStrictEqual(callOrder, ['version', 'clone', 'diff', 'pr-comment', 'export', 'to-parquet', 'validate']);

    for (const [name, m] of Object.entries(mocks)) {
      assert.strictEqual(m.mock.callCount(), 1, `${name} handler should be called exactly once`);
    }
  });

  it('should stop and throw if a step fails', async () => {
    const callOrder: string[] = [];
    const mocks = mockAllHandlers(callOrder);
    mocks['diff'] = mock.method(DiffCommand, 'handler', async () => {
      callOrder.push('diff');
      throw new Error('Diff failed');
    });

    await assert.rejects(() => FlowCommand.handler({ ...defaultFlowArgs }), { message: 'Diff failed' });

    assert.deepStrictEqual(callOrder, ['version', 'clone', 'diff']);
  });

  describe('step arguments: clone', () => {
    it('should pass repository and ref to clone', async () => {
      const { clone } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs, ref: 'feature-branch' });

      const cloneCall = clone.mock.calls[0];
      assert.ok(cloneCall, 'clone handler should have been called');
      const cloneArgs = cloneCall.arguments[0];
      assert.ok(cloneArgs, 'clone handler should have received arguments');
      assert.ok(cloneArgs.repository, 'clone args should have a repository');
      assert.ok(cloneArgs.ref, 'clone args should have a ref');
      assert.strictEqual(cloneArgs.repository, 'linz/topographic-test-data');
      assert.strictEqual(cloneArgs.ref, 'feature-branch');
    });
    it('should pass default ref "master" to clone when ref is not specified', async () => {
      const { clone } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs, ref: 'master' });

      const cloneArgs = clone.mock.calls[0]?.arguments[0];
      assert.ok(cloneArgs, 'clone handler should have been called');
      assert.strictEqual(cloneArgs.ref, 'master');
    });
  });

  describe('step arguments: diff', () => {
    it('should pass clone output as context to diff', async () => {
      const { clone, diff } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs });

      const cloneArgs = clone.mock.calls[0]?.arguments[0];
      assert.ok(cloneArgs, 'clone handler should have received arguments');
      assert.ok(cloneArgs.output, 'clone args should have an output');

      const diffArgs = diff.mock.calls[0]?.arguments[0];
      assert.ok(diffArgs, 'diff handler should have received arguments');
      assert.ok(diffArgs.context, 'diff args should have a context');

      assert.strictEqual(diffArgs.context.href, cloneArgs.output.href);
    });
  });

  describe('step arguments: pr-comment', () => {
    it('should pass diff summary file as body to pr-comment', async () => {
      const { diff, comment } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs });

      const diffArgs = diff.mock.calls[0]?.arguments[0];
      assert.ok(diffArgs, 'diff handler should have received arguments');
      assert.ok(diffArgs.summaryFile, 'diff args should have a summaryFile');

      const commentArgs = comment.mock.calls[0]?.arguments[0];
      assert.ok(commentArgs, 'comment handler should have received arguments');

      const bodyFile = commentArgs.bodyFile;
      assert.ok(bodyFile, 'pr-comment should receive at least one body file');
      assert.strictEqual(bodyFile.href, diffArgs.summaryFile.href);
    });
  });

  describe('step arguments: export', () => {
    it('should pass clone output as context to export', async () => {
      const { clone, export: exportMock } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs });

      const cloneArgs = clone.mock.calls[0]?.arguments[0];
      assert.ok(cloneArgs, 'clone handler should have received arguments');
      assert.ok(cloneArgs.output, 'clone args should have an output');

      const exportArgs = exportMock.mock.calls[0]?.arguments[0];
      assert.ok(exportArgs, 'export handler should have received arguments');
      assert.ok(exportArgs.context, 'export args should have a context');

      assert.strictEqual(exportArgs.context.href, cloneArgs.output.href);
    });

    it('should pass changedDatasetsOnly to export', async () => {
      const { export: exportMock } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs, changedDatasetsOnly: true });

      const exportArgs = exportMock.mock.calls[0]?.arguments[0];
      assert.ok(exportArgs, 'export handler should have received arguments');
      assert.ok(exportArgs.changed, 'export args should have a changed flag');
      assert.strictEqual(exportArgs.changed, true);
    });
  });

  describe('step arguments: to-parquet', () => {
    it('should pass export output as source to to-parquet', async () => {
      const { export: exportMock, parquet } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs });

      const exportArgs = exportMock.mock.calls[0]?.arguments[0];
      assert.ok(exportArgs, 'export handler should have received arguments');
      assert.ok(exportArgs.output, 'export args should have an output');

      const parquetArgs = parquet.mock.calls[0]?.arguments[0];
      assert.ok(parquetArgs, 'to-parquet handler should have received arguments');

      assert.ok(parquetArgs.sourceFiles.length > 0, 'to-parquet should receive source files');
      const firstSourceFile = parquetArgs.sourceFiles[0];
      assert.ok(firstSourceFile, 'first source file should exist');
      assert.strictEqual(firstSourceFile.href, exportArgs.output.href);
    });

    it('should pass output to to-parquet', async () => {
      const { parquet } = mockAllHandlers();

      const output = stringToUrlFolder('/tmp/test-flow-output');
      await FlowCommand.handler({ ...defaultFlowArgs, output });

      const parquetArgs = parquet.mock.calls[0]?.arguments[0];
      assert.ok(parquetArgs, 'to-parquet handler should have received arguments');
      assert.ok(parquetArgs.output, 'parquet args should have an output');
      assert.strictEqual(parquetArgs.output.href, output.href);
    });
  });

  describe('step arguments: validate schema', () => {
    const schemaTestDir = path.join(tmpdir(), 'kart-flow-schema-test');
    const parquetDir = path.join(schemaTestDir, 'parquet', 'files.parquet');
    const schemaDir = path.join(schemaTestDir, 'schema');

    before(async () => {
      await mkdir(parquetDir, { recursive: true });
      await mkdir(schemaDir, { recursive: true });
      await writeFile(path.join(parquetDir, 'road_line.parquet'), '');
      await writeFile(path.join(parquetDir, 'water.parquet'), '');
      await writeFile(path.join(schemaDir, 'road_line.json'), '{}');
      await writeFile(path.join(schemaDir, 'water.json'), '{}');
    });

    after(async () => {
      await rm(schemaTestDir, { recursive: true, force: true });
    });

    it('should validate each parquet file against its matching schema json', async () => {
      const mocks = mockAllHandlers();
      const validateSchema = mock.method(ValidateSchemaCommand, 'handler', async () => {});

      await FlowCommand.handler({
        ...defaultFlowArgs,
        parquetTempLocation: stringToUrlFolder(path.join(schemaTestDir, 'parquet')),
        schemaDirectory: stringToUrlFolder(schemaDir),
      });

      assert.strictEqual(validateSchema.mock.callCount(), 2);

      const schemaCalls = validateSchema.mock.calls
        .map((c) => c.arguments[0])
        .map((a) => ({ schema: path.basename(a?.schema?.pathname ?? ''), parquet: path.basename(a?.paths?.[0]?.pathname ?? '') }))
        .sort((a, b) => a.parquet.localeCompare(b.parquet));

      assert.deepStrictEqual(schemaCalls, [
        { schema: 'road_line.json', parquet: 'road_line.parquet' },
        { schema: 'water.json', parquet: 'water.parquet' },
      ]);
      assert.strictEqual(mocks.validate.mock.callCount(), 1, 'validate should run after schema step succeeds');
    });

    it('should run all schema validations and fail if any validation fails', async () => {
      const mocks = mockAllHandlers();
      const validateSchema = mock.method(ValidateSchemaCommand, 'handler', async (args: { schema: URL }) => {
        if (args.schema.pathname.endsWith('/road_line.json')) throw new Error('validation failed');
      });

      await assert.rejects(
        () =>
          FlowCommand.handler({
            ...defaultFlowArgs,
            parquetTempLocation: stringToUrlFolder(path.join(schemaTestDir, 'parquet')),
            schemaDirectory: stringToUrlFolder(schemaDir),
          }),
        { message: 'Schema validation failed for 1 parquet file(s)' },
      );

      assert.strictEqual(validateSchema.mock.callCount(), 2, 'all schema validations should be attempted');
      assert.strictEqual(mocks.validate.mock.callCount(), 0, 'validate should not run when schema step fails');
    });

    it('should fail when schema is missing for a parquet file', async () => {
      const mocks = mockAllHandlers();
      const validateSchema = mock.method(ValidateSchemaCommand, 'handler', async () => {});

      await assert.rejects(
        () =>
          FlowCommand.handler({
            ...defaultFlowArgs,
            parquetTempLocation: stringToUrlFolder(path.join(schemaTestDir, 'parquet')),
            schemaDirectory: stringToUrlFolder(path.join(schemaTestDir, 'empty-schema')),
          }),
        { message: /Schema validation failed/ },
      );

      assert.strictEqual(validateSchema.mock.callCount(), 0, 'no validations attempted when schemas missing');
      assert.strictEqual(mocks.validate.mock.callCount(), 0, 'validate should not run when schema step fails');
    });
  });

  describe('step arguments: validate', () => {
    it('should pass parquet temp location to validate db-path', async () => {
      const { parquet, validate } = mockAllHandlers();

      await FlowCommand.handler({ ...defaultFlowArgs });

      const parquetArgs = parquet.mock.calls[0]?.arguments[0];
      assert.ok(parquetArgs, 'to-parquet handler should have received arguments');
      assert.ok(parquetArgs.tempLocation, 'parquet args should have a tempLocation');

      const validateArgs = validate.mock.calls[0]?.arguments[0];
      assert.ok(validateArgs, 'validate handler should have received arguments');
      assert.ok(validateArgs['db-path'], 'validate args should have a db-path');

      const expectedDbPath = new URL('files.parquet', parquetArgs.tempLocation);
      assert.strictEqual(validateArgs['db-path'].href, expectedDbPath.href);
    });

    it('should pass output to validate', async () => {
      const { validate } = mockAllHandlers();

      const output = stringToUrlFolder('/tmp/test-flow-output');
      await FlowCommand.handler({ ...defaultFlowArgs, output });

      const validateArgs = validate.mock.calls[0]?.arguments[0];
      assert.ok(validateArgs, 'validate handler should have received arguments');
      assert.ok(validateArgs.output, 'validate args should have an output');
      assert.strictEqual(validateArgs.output.href, output.href);
    });
  });
});
