import type { EmittedSourceFile, SourceFile, TypeEmitter } from '@typespec/asset-emitter';
import type { JSONSchemaEmitterOptions } from '@typespec/json-schema';

declare module '@typespec/json-schema' {
  /** `versionStr` isn't a real upstream option — it's injected by `VersionedJsonSchemaEmitter` below to suffix a release's `$id`s. */
  export type JsonSchemaEmitterOutputOptions = JSONSchemaEmitterOptions & { versionStr?: string };

  export class JsonSchemaEmitter extends TypeEmitter<Record<string, unknown>, JsonSchemaEmitterOutputOptions> {
    sourceFile(sourceFile: SourceFile<Record<string, unknown>>): EmittedSourceFile;
  }
}
