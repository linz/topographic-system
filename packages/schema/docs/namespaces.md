# Organizing schemas with namespaces

This describes the convention for splitting models across TypeSpec namespaces as the schema
grows beyond `NzTopo50`/`NzTopo250` into other datasets (`Addressing`, `Property`,
`BuildingOutlines`, ...). It relies on `@linzjs/typespec-emitter`'s namespace/version planning
(`packages/typespec-emitter/src/plan.ts`, consumed by every emit target) — see that package's
README for the emitter options themselves.

## The convention

```
Linz.Topography                    — shared base models, mixins, common enums
Linz.Topography.NzTopo50           — product: NZ Topo 1:50k models
Linz.Topography.NzTopo250          — product: NZ Topo 1:250k models
Linz.Topography.Addressing         — dataset: addressing models
Linz.Topography.Property           — dataset: property/parcel models
Linz.Topography.BuildingOutlines   — dataset: building outline models
```

`Linz.Topography` is the root. Each **direct child namespace** of it is treated as an
independent, separately-released dataset ("product"). Models declared directly on
`Linz.Topography` itself (not inside any child) are common/shared and get pulled into every
product automatically.

This is a flat list of siblings, not a deep tree — see [One level of fan-out](#one-level-of-fan-out-only)
below for why.

## What the emitter does with this

Every emit target (`json-schema`, `typescript`, `parquet`, `pydantic`, `stac`) writes into a
Hive-style `namespace={dotted path}/` directory per namespace, with `release=v{version}/` nested
inside it if `Linz.Topography` is versioned via `@typespec/versioning` (see
`packages/typespec-emitter/src/plan.ts`). The _shared_ root (`Linz.Topography`'s own models) gets
its own `namespace=linz.topography/` directory alongside each product's — it isn't only folded
into them:

```
schema/
  catalog.json                                      — links every namespace below
  namespace=linz.topography/                         — shared models, standalone
    catalog.json                                     — links its own releases
    release=v1/
      collection.json
      theme.json
    release=v2/
      collection.json
      theme.json
  namespace=linz.topography.nztopo50/
    catalog.json
    release=v1/
      collection.json
      airport.json                                  — shared models (theme.json) folded in too,
      theme.json                                        so $refs between them resolve locally
    release=v2/
      ...
  namespace=linz.topography.nztopo250/
    catalog.json
    release=v1/
      collection.json
      airport.json
      theme.json
    release=v2/
      ...
```

If `Linz.Topography` isn't versioned, each `namespace=.../` directory holds a `collection.json`
directly instead of a `release=v{version}/` tree. If `Linz.Topography` has no child namespaces at
all (today's schema, before any product namespaces exist), there's nothing to disambiguate and
everything emits flat with no `namespace=`/`release=` wrapping — that's the current, unchanged
behaviour for the not-yet-namespaced schema.

Shared models are **duplicated** into every product's directory (not just linked), because
`@typespec/json-schema`'s `$ref`s assume the referenced id's file is a sibling in the same
directory — a cross-directory `$ref` from a product's schema back to `namespace=linz.topography/`
wouldn't resolve. The standalone `namespace=linz.topography/` copy exists for anyone who wants
just the shared layer on its own.

### `@jsonSchema` ids only need to be unique per namespace

Because each namespace's schemas land in their own directory, `NzTopo50.airport` and
`NzTopo250.airport` can both use `@jsonSchema("airport")` without colliding — the directory is
what disambiguates them, not the id. Don't bother prefixing ids with the product name (no need
for `nztopo50_airport`); keep them as plain, readable names.

### One level of fan-out only

The emitter only looks at the _direct_ children of `Linz.Topography` to decide what gets its own
output directory. Any namespace nesting _inside_ a product (e.g.
`Linz.Topography.BuildingOutlines.Something`) is purely organizational — it does not create
another output directory. Everything under a product namespace, however deeply nested, is
collected into that one product's output. Keep that in mind when deciding whether something is a
new product (→ direct child of `Linz.Topography`) or an internal grouping within an existing one
(→ nested namespace or just a separate file, same namespace).

## Adding a new, simple dataset (e.g. Addressing, Property)

1. Create a folder: `src/topography/addressing/`.
2. Give every file in it the same file-scoped namespace declaration:
   ```typespec
   namespace Linz.Topography.Addressing;
   ```
3. Reference shared models from `Linz.Topography` unqualified — namespace lookup walks up through
   enclosing namespaces, so `TopographicFeature`, `TopographicMetadata`, `Theme`, etc. resolve
   without an explicit `using` or qualification, the same way `NzTopo50`/`NzTopo250` already do.
4. Tag each model with a plain, descriptive `@jsonSchema("...")` id — no product prefix needed
   (see above).
5. Import the new folder's files from wherever `Linz.Topography`'s files are already being
   imported, so the compiler picks it up.

`Property` follows the identical pattern in `src/topography/property/`.

## Adding a complex dataset (BuildingOutlines)

`BuildingOutlines` isn't one flat list of fields — it's likely to span multiple related concepts
(the outline geometry itself, building parts/levels, roof structure, attributes). Treat it as
**one product namespace** (`Linz.Topography.BuildingOutlines`) split across **multiple files**,
not multiple products:

```
src/topography/building_outlines/
  main.tsp          — imports the rest of this folder
  outline.tsp        — the primary outline model(s)
  part.tsp            — building parts / levels
  roof.tsp            — roof structure
  common.tsp          — enums/mixins used only within BuildingOutlines
```

Every file declares `namespace Linz.Topography.BuildingOutlines;`. If a sub-concept genuinely
needs its own scope to avoid name clashes within the dataset (e.g. an enum called `Status` that
would otherwise clash with another `Status` in the same namespace), nest it one level further
(`Linz.Topography.BuildingOutlines.Roof`) — this stays organizational only; it still emits into
the single `building_outlines/` output directory alongside everything else in the product, per
[One level of fan-out](#one-level-of-fan-out-only).

Don't split `BuildingOutlines` into several _sibling_ products (e.g. `BuildingOutlines` +
`BuildingOutlineParts` as two children of `Linz.Topography`) unless they are genuinely meant to
be released/versioned/consumed independently of each other — that's the real test for "is this a
new product" vs. "is this organization within an existing one".

## Checklist for a new dataset

- **Is it released/consumed as its own schema set, independent of existing products?**
  → new direct child of `Linz.Topography` (new product).
- **Is it a field, mixin, or enum used by more than one product?**
  → declare it directly on `Linz.Topography` (shared), not inside any product namespace.
- **Is it an internal grouping within one product's own models?**
  → a nested namespace or just another file under that product — not a new top-level namespace.
