import { emitFile } from '@typespec/compiler';
import type { Model, Program } from '@typespec/compiler';
import { isJsonSchemaDeclaration } from '@typespec/json-schema';

import { toPascalCase } from './utils.ts';

export async function emitPydanticModels(program: Program, outputFile: string, models: Map<string, Model>) {
  // Placeholder for future Pydantic models generation
  let code = `# This is a placeholder for Python Pydantic models output.\n\n`;
  for (const [name, model] of models.entries()) {
    if (!isJsonSchemaDeclaration(program, model)) {
      continue;
    }
    code += `class ${toPascalCase(name)}:\n    pass\n\n`;
  }
  await emitFile(program, {
    path: outputFile,
    content: code,
  });
}
