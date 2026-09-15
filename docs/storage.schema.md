# Dataset Schema

All datasets have a corresponding schema defined as [Typespec](https://typespec.io)

## Why Typespec

Typespec allows us to create the schema once, and export it into many formats we use:

- JSONSchema - Defines the shape of the data
- Parquet - Physical storage format
- Markdown - Documentation
- Typescript - NPM module to import types
- Pydantic (Future) - Python module to import types

## Kart integration

Datasets are attached to their schemas using `kart meta`

```bash
# Set the schema for a dataset
kart meta set ${dataset_name} schema ${schema_host}/schema/release=v0.0.1/${dataset_name}.json

# Get the schema for a dataset
kart meta get ${dataset_name} schema
```

These schemas are critical in ensuring the dataset structure is aligned to LINZ best practices and that all data published is aligned to the schema itself, The schemas also store valuable metadata about the dataset

## Key fields

Ideally all datasets should contain three key meta fields

- id - `uuidv7` - Unique ID with the creation time of the feature
- created_at - `datetime` - Creation time of the feature
- updated_at - `datetime` - Last modified time of the feature

## Versioning

Schemas are versioned using Typespec's versioning structure, with all versions of the schemas stored in the [schema](../packages/schema/README.md).

CI needs to validate that the historical schemas are not being modified, updates to descriptions are allowed but changing the schema is not.

## Emitters

### JSON Schema

JSON schemas are stored in a public https accessible location with the following structure:

```
/schema/latest/airport.json # points to v2.1
/schema/release=v1.1/airport.json
/schema/release=v1.2/airport.json
/schema/release=v2.1/airport.json
```

### Typescript

All schemas and their types are published into npm `@linzjs/topographic-schema`

```typescript
import type {Airport} from '@linzjs/topographic-schema'; // Latest

const airport: Airport = { ... };

import type {Airport as AirportV1_1} from '@linzjs/topographic-schema/v1.1'; // Specific version
```

### Parquet JSON

JSON schema is not expressive enough to specify the parquet layout of the dataset, so a Parquet JSON is created, based off the [typespec proposal](https://github.com/microsoft/typespec/issues/10334)

```json
{

  "type": "message",
  "name": "airport",
  "fields": [
    { "name": "id", "repetition": "REQUIRED", "physical_type": "FIXED_LEN_BYTE_ARRAY", "type_length":16, "logical_type": "UUID" },
    { "name": "created_at", "repetition": "REQUIRED", "physical_type": "INT64", "logical_type": "TIMESTAMP_MILLIS" },
    { "name": "updated_at", "repetition": "OPTIONAL", "physical_type": "INT64", "logical_type": "TIMESTAMP_MILLIS" },
    { "name": "name", "repetition": "REQUIRED", "physical_type": "BYTE_ARRAY", "logical_type": "UTF8" },
    { "name": "geometry", "repetition": "REQUIRED", "physical_type": "BYTE_ARRAY", "logical_type": "GEOMETRY" }
  ],
  "row_group_size": 8000,
  "compression": "zstd",
  "compression_level": 3
}
```
