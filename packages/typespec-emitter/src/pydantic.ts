import { emitFile, getDirectoryPath, joinPaths } from '@typespec/compiler';
import type { EmitContext, Enum, Model, Union } from '@typespec/compiler';
import { isJsonSchemaDeclaration } from '@typespec/json-schema';

import { planEmission, unitOutputDir } from './plan.ts';
import { collectOwnTypes, collectTypes, toPascalCase } from './utils.ts';

/**
 * Emits placeholder Pydantic models, one `models.py` per (version × product) unit — see
 * `planEmission`. Each unit combines its own namespace's types with any declared directly on the
 * shared root at that version.
 */
export async function emitPydanticModels(context: EmitContext<any>, outputTarget: string): Promise<void> {
  const { program } = context;
  const outputDir = outputTarget.endsWith('.py') ? getDirectoryPath(outputTarget) : outputTarget;

  for (const unit of planEmission(program)) {
    const models = new Map<string, Model>();
    const enums = new Map<string, Enum>();
    const unions = new Map<string, Union>();

    if (unit.sharedNs) collectOwnTypes(unit.sharedNs, models, enums, unions);
    if (unit.ownOnly) {
      collectOwnTypes(unit.ns, models, enums, unions);
    } else {
      collectTypes(unit.ns, models, enums, unions);
    }

    // Placeholder for future Pydantic models generation
    let code = `# This is a placeholder for Python Pydantic models output.\n\n`;
    for (const [name, model] of models.entries()) {
      if (!isJsonSchemaDeclaration(program, model)) {
        continue;
      }
      code += `class ${toPascalCase(name)}:\n    pass\n\n`;
    }

    const targetFile =
      !unit.path && outputTarget.endsWith('.py')
        ? outputTarget
        : joinPaths(unitOutputDir(outputDir, unit.path), 'models.py');
    await emitFile(program, { path: targetFile, content: code });
  }
}
