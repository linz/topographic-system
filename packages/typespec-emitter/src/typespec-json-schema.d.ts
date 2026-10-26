import type { TypeEmitter } from '@typespec/asset-emitter';

declare module '@typespec/json-schema' {
  export class JsonSchemaEmitter extends TypeEmitter<any, any> {
    sourceFile(sourceFile: any): any;
  }
}
