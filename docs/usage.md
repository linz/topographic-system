# NZ Topographic Schema Usage Guide

## Quick Start

### Installation

```bash
pip install -r requirements.txt
```

### Basic Usage

```python
from topographic_schema.core.models import TopoFeature
from topographic_schema.core.geometry import Geometry, GeometryType
from uuid import uuid4

# Create a basic feature
feature = TopoFeature(
    topo_id=uuid4(),
    theme="transport",
    type="bridge",
    geometry=Geometry(
        type=GeometryType.POINT,
        coordinates=[174.7762, -41.2865]
    ),
    version=1
)
```

### Working with Features

#### Creating Features

Features are created by instantiating the appropriate model class:

```python
from topographic_schema.transport.models import BridgeLine
from topographic_schema.core.geometry import Geometry, GeometryType

bridge = BridgeLine(
    topo_id=uuid4(),
    theme="transport", 
    type="bridge",
    geometry=Geometry(
        type=GeometryType.LINESTRING,
        coordinates=[[174.7762, -41.2865], [174.7765, -41.2862]]
    ),
    name="Wellington Harbour Bridge",
    status="active",
    bridge_use="ROAD"
)
```

#### Validation

Pydantic automatically validates all fields:

```python
try:
    # This will fail validation
    invalid_bridge = BridgeLine(
        theme="transport",
        type="bridge"
        # Missing required fields
    )
except ValidationError as e:
    print(f"Validation error: {e}")
```

#### Serialization

Convert features to dictionaries or JSON:

```python
# To dictionary
bridge_dict = bridge.model_dump()

# To JSON
bridge_json = bridge.model_dump_json(indent=2)
```

### Extension Fields

The schema supports extension fields following OvertureMaps patterns:

```python
bridge_with_extensions = BridgeLine(
    topo_id=uuid4(),
    theme="transport",
    type="bridge",
    geometry=geometry,
    ext_custom_field="custom_value",  # Valid extension
    # invalid_field="not_allowed"     # Would raise ValidationError
)
```

### Geometry Constraints

Features enforce geometry type constraints:

```python
# This works - LineString is allowed for bridges
bridge = BridgeLine(
    geometry=Geometry(
        type=GeometryType.LINESTRING,
        coordinates=[[0, 0], [1, 1]]
    )
)

# This would fail - Point is not allowed for bridge lines
# bridge = BridgeLine(
#     geometry=Geometry(
#         type=GeometryType.POINT,
#         coordinates=[0, 0]
#     )
# )
```

## Advanced Usage

### Custom Features  

Create custom features by extending base classes:

```python
from typing import Literal
from topographic_schema.core import TopoFeature, Named

class CustomFeature(TopoFeature[Literal["custom"], Literal["feature"]], Named):
    """Custom feature type."""
    
    custom_field: str | None = None
```

### Mixins

Use mixins to add common functionality:

```python
from topographic_schema.core import Named, WithStatus, WithLevel

class MyFeature(TopoFeature, Named, WithStatus, WithLevel):
    """Feature with names, status, and level."""
    pass
```

### JSON Schema

Generate JSON schemas for integration:

```python
# Get JSON schema for a model
schema = BridgeLine.model_json_schema()

# Use for validation in other systems
import jsonschema
jsonschema.validate(bridge_dict, schema)
```

## Best Practices

1. **Always validate data** - Let Pydantic catch errors early
2. **Use type hints** - Leverage Python's type system  
3. **Document custom fields** - Add descriptions to Field annotations
4. **Follow naming conventions** - Use consistent field and class names
5. **Test your schemas** - Write tests for validation logic

## Integration Examples

### Database Integration

```python
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

# Create table from Pydantic model
def create_table_from_model(model_class):
    columns = []
    for field_name, field_info in model_class.model_fields.items():
        if field_name == 'topo_id':
            columns.append(sa.Column('topo_id', UUID, primary_key=True))
        elif field_name == 'geometry':
            # Use PostGIS geometry type
            continue  
        else:
            columns.append(sa.Column(field_name, JSONB))
    
    return sa.Table(model_class.__name__.lower(), sa.MetaData(), *columns)
```

### FastAPI Integration

```python
from fastapi import FastAPI
from topographic_schema.transport.models import BridgeLine

app = FastAPI()

@app.post("/bridges/")
async def create_bridge(bridge: BridgeLine):
    # Pydantic handles validation automatically
    return {"message": f"Created bridge {bridge.name}"}
```

## Migration Guide

### From Legacy Schema

If migrating from existing systems:

```python
def migrate_legacy_feature(legacy_data):
    """Convert legacy feature data to new schema."""
    return BridgeLine(
        topo_id=legacy_data['id'],
        t50_fid=legacy_data.get('t50_fid'),
        theme="transport",
        type="bridge", 
        geometry=convert_geometry(legacy_data['geom']),
        name=legacy_data.get('name'),
        # Map other fields as needed
    )
```
