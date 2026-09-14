# Dataset Schema

All datasets have a corresponding schema defined as [Typespec](todo:url)

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
