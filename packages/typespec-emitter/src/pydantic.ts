import { emitFile } from '@typespec/compiler';
import type { Model } from '@typespec/compiler';

import { toPascalCase } from './utils.ts';

export async function emitPydanticModels(program: any, outputFile: string, models: Map<string, Model>) {
  // Placeholder for future Pydantic models generation
  let code = `# This is a placeholder for Python Pydantic models output.\n\n`;
  for (const model of models.keys()) {
    code += `class ${toPascalCase(model)}:\n    pass\n\n`;
  }
  await emitFile(program, {
    path: outputFile,
    content: code,
  });
}
