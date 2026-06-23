import { traceAndRun } from '@linzjs/topographic-system-shared';
import { StacPushCommand } from '@linzjs/topographic-system-stac';
import { subcommands } from 'cmd-ts';

import packageJson from '../package.json' with { type: 'json' };
import { CloneCommand } from './cli/action.clone.ts';
import { DiffCommand } from './cli/action.diff.ts';
import { ExportCommand } from './cli/action.export.ts';
import { FlowCommand } from './cli/action.flow.ts';
import { IceContourCommand } from './cli/action.ice.contour.ts';
import { LintQgisProjectCommand } from './cli/action.lint.qgis.ts';
import { CommentCommand } from './cli/action.pr.comment.ts';
import { RockLineCommand } from './cli/action.rock.line.ts';
import { ParquetCommand } from './cli/action.to.parquet.ts';
import { ValidateSchemaCommand } from './cli/action.validate.schema.ts';
import { ValidateCommand } from './cli/action.validate.ts';
import { VersionCommand } from './cli/action.version.ts';

const cmds = {
  'kart-prepare': FlowCommand,
  clone: CloneCommand,
  diff: DiffCommand,
  export: ExportCommand,
  'to-parquet': ParquetCommand,
  'pr-comment': CommentCommand,
  validate: ValidateCommand,
  version: VersionCommand,
  'ice-contour': IceContourCommand,
  'rock-line': RockLineCommand,
  'stac-push': StacPushCommand,
  'validate-schema': ValidateSchemaCommand,
  'lint-qgis': LintQgisProjectCommand,
};

const Cli = subcommands({
  name: 'topographic-system',
  description: '',
  cmds,
});

void traceAndRun(Cli, cmds, packageJson.name);
