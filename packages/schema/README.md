# Topographic System Schema Changes

## Versioning Model

Schema versions are managed via [`@typespec/versioning`](https://typespec.io/docs/libraries/versioning/reference).

- Versions are defined in [`src/version.tsp`](file:///home/blacha/git/linz/topographic-system/packages/schema/src/version.tsp):
  ```typespec
  enum Versions {
    v0_3: "0.3",
    v0_4: "0.4",
  }
  ```
- All feature definitions reside under [`src/features/`](file:///home/blacha/git/linz/topographic-system/packages/schema/src/features/) within `namespace Topography;`.
- Rather than copying files into separate folders for upcoming changes, use versioning decorators such as `@added(Versions.v0_4)` or `@madeOptional(Versions.v0_4)`.

### Example

```typespec
using TypeSpec.Versioning;

namespace Topography;

@jsonSchema("water")
model Water {
  id: string;
  name?: string;

  @added(Versions.v0_4)
  new_attribute?: string;
}
```

## Compiling & Bundling

From the repository root or the `packages/schema` directory, run:

```bash
npm --prefix packages/schema run bundle
```

This command executes:

1. `tsp compile .` using [`tspconfig.yaml`](file:///home/blacha/git/linz/topographic-system/packages/schema/tspconfig.yaml) and the custom emitter [`@linzjs/typespec-emitter`](file:///home/blacha/git/linz/topographic-system/packages/typespec-emitter).
2. Generates versioned JSON schemas for each release into `schema/release=v{version}/*.json` (e.g., `schema/release=v0.3/`, `schema/release=v0.4/`).
3. Generates TypeScript type definitions into [`src/types/index.ts`](file:///home/blacha/git/linz/topographic-system/packages/schema/src/types/index.ts).
4. Automatically formats the generated JSON schema files and TypeScript types with `oxfmt`.
