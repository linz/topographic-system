# @linzjs/typespec-emitter

Custom TypeSpec emitter for LINZ Topographic System schemas.

## Features

- **TypeScript Types**: Emits TypeScript type aliases for enums and unions, and interfaces for models with JSDoc documentation comments.
- **Parquet Schemas**: Emits Parquet JSON schema descriptions mapping TypeSpec scalars, arrays, and structs to Parquet physical and logical types.
- **Pydantic Models**: Experimental / future support for Python Pydantic models.

## Configuration

Configure the emitter in your `tspconfig.yaml`:

```yaml
emit:
  - '@linzjs/typespec-emitter'
options:
  '@linzjs/typespec-emitter':
    typescript-output-file: '{project-root}/src/types/index.ts'
    parquet-output-dir: '{project-root}/src/parquet'
    pydantic-output-file: '{project-root}/src/models.py'
```

### Options

| Option                   | Type     | Description                                      |
| ------------------------ | -------- | ------------------------------------------------ |
| `typescript-output-file` | `string` | File path for emitted TypeScript types.          |
| `parquet-output-dir`     | `string` | Directory path for emitted Parquet JSON schemas. |
| `pydantic-output-file`   | `string` | File path for emitted Pydantic Python models.    |
