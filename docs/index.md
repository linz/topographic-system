# NZ Topographic Schema Documentation

Welcome to the NZ Topographic Schema documentation. This schema provides Pydantic models for New Zealand's Topo50 data, following [OvertureMaps](https://github.com/OvertureMaps/schema) patterns and best practices.

*Generated on 2026-03-24 13:31:58*

## Quick Links

- **[Usage Guide](usage.md)** - Get started with the schema
- **[API Reference](api/index.md)** - Complete model documentation  
- **[Examples](examples.md)** - Code examples and patterns
- **[JSON Schemas](schemas/)** - Machine-readable schemas

## Architecture

The schema is organized into themes reflecting New Zealand's topographic data structure:

### 🚗 Transport & Infrastructure
Roads, bridges, railways, tunnels, and utility infrastructure.

### 🏠 Buildings & Structures  
Buildings, structures, and residential areas.

### 🌊 Land & Water
Land cover, water bodies, coastlines, and wetlands.

### ⛰️ Relief & Physical
Contours, elevations, cliffs, and physical features.

### 📝 Annotation
Text labels, place names, and cartographic annotations.

## Key Features

- **Type Safety** - Full Pydantic validation with type hints
- **Geometry Constraints** - Enforce valid geometry types per feature
- **Extension Support** - Allow custom fields with `ext_*` pattern 
- **NZ Compatibility** - Support for Topo50 legacy identifiers
- **Māori Names** - Built-in support for macronated text
- **Version Tracking** - Change management and provenance

## Standards Compliance

- **GeoJSON Compatible** - Standard geometry representation
- **OvertureMaps Inspired** - Following proven geospatial patterns  
- **Pydantic V2** - Modern Python validation framework
- **LINZ Standards** - Aligned with NZ data standards

## Getting Started

```python
from topographic_schema.transport.models import BridgeLine
from topographic_schema.core.geometry import Geometry, GeometryType
from uuid import uuid4

# Create a bridge feature
bridge = BridgeLine(
    topo_id=uuid4(),
    theme="transport",
    type="bridge",
    geometry=Geometry(
        type=GeometryType.LINESTRING, 
        coordinates=[[174.7762, -41.2865], [174.7765, -41.2862]]
    ),
    name="Wellington Harbour Bridge",
    status="active"
)

# Validate and serialize
print(bridge.model_dump_json(indent=2))
```

## Documentation Structure

```
docs/
├── index.md              # This page
├── usage.md              # Usage guide and examples  
├── api/                  # API reference
│   ├── index.md          # API overview
│   ├── transport.md      # Transport features
│   ├── buildings.md      # Building features
│   └── ...               # Other themes
├── schemas/              # JSON Schema files  
│   ├── transport/        # Transport schemas
│   └── ...               # Other theme schemas
└── examples.md           # Code examples
```

Need help? Check the [usage guide](usage.md) or browse the [API reference](api/index.md).
