# Examples

Practical examples of using the NZ Topographic Schema.

## Basic Examples

### Creating a Bridge

```python
from topographic_schema.transport.models import BridgeLine
from topographic_schema.core.geometry import Geometry, GeometryType
from uuid import uuid4

bridge = BridgeLine(
    topo_id=uuid4(),
    theme="transport",
    type="bridge", 
    geometry=Geometry(
        type=GeometryType.LINESTRING,
        coordinates=[
            [174.7762, -41.2865],  # Start point
            [174.7765, -41.2862]   # End point
        ]
    ),
    name="Example Bridge",
    name_ascii="Example Bridge",
    macronated="N",
    status="operational",
    visibility="visible",
    bridge_use="ROAD",
    construction_type="concrete",
    height=15.5,
    version=1
)

print(f"Created bridge: {bridge.name}")
print(f"Bridge ID: {bridge.topo_id}")
```

### Working with Geometry

```python
from topographic_schema.core.geometry import Geometry, GeometryType

# Point geometry
point_geom = Geometry(
    type=GeometryType.POINT,
    coordinates=[174.7762, -41.2865]
)

# LineString geometry  
line_geom = Geometry(
    type=GeometryType.LINESTRING,
    coordinates=[
        [174.0, -41.0],
        [174.1, -41.1], 
        [174.2, -41.2]
    ]
)

# Polygon geometry
polygon_geom = Geometry(
    type=GeometryType.POLYGON,
    coordinates=[[
        [174.0, -41.0],
        [174.1, -41.0],
        [174.1, -41.1],
        [174.0, -41.1],
        [174.0, -41.0]  # Closed ring
    ]]
)
```

### Using Mixins

```python
from typing import Literal
from topographic_schema.core import TopoFeature, Named, WithStatus, WithLevel

class CustomPoint(
    TopoFeature[Literal["infrastructure"], Literal["point"]],
    Named,
    WithStatus, 
    WithLevel
):
    """Custom point feature with naming, status, and level."""
    
    # Custom fields
    importance: int | None = None

# Create instance
custom_point = CustomPoint(
    topo_id=uuid4(),
    theme="infrastructure",
    type="point",
    geometry=point_geom,
    name="Important Point",           # From Named
    status="active",                  # From WithStatus 
    level=1,                         # From WithLevel
    importance=5                     # Custom field
)
```

## Advanced Examples

### Extension Fields

```python
# Extension fields must start with 'ext_'
bridge_with_extensions = BridgeLine(
    topo_id=uuid4(),
    theme="transport",
    type="bridge",
    geometry=line_geom,
    
    # Standard fields
    name="Extended Bridge",
    
    # Extension fields (custom data)
    ext_engineering_report="Report_2024_001.pdf",
    ext_last_inspection="2024-03-15",
    ext_load_capacity_tonnes=50.0,
    ext_maintenance_contractor="ABC Engineering"
)

# This would raise ValidationError - invalid extension field name
# bridge_with_bad_ext = BridgeLine(
#     ...,
#     custom_field="not allowed"  # Must start with 'ext_'
# )
```

### Validation Examples

```python
from pydantic import ValidationError

# Valid feature
try:
    valid_bridge = BridgeLine(
        topo_id=uuid4(),
        theme="transport",
        type="bridge", 
        geometry=line_geom,
        version=1  # Required field
    )
    print("✓ Bridge created successfully")
except ValidationError as e:
    print(f"✗ Validation failed: {e}")

# Invalid feature - missing required fields
try:
    invalid_bridge = BridgeLine(
        theme="transport",
        type="bridge"
        # Missing topo_id, geometry, version
    )
except ValidationError as e:
    print(f"✗ Expected validation error: {e}")
```

### Serialization Examples

```python
# Convert to dictionary
bridge_dict = bridge.model_dump()

# Convert to JSON
bridge_json = bridge.model_dump_json(indent=2)

# Convert excluding certain fields
bridge_minimal = bridge.model_dump(exclude={'ext_*', 'create_date'})

# Convert including only certain fields  
bridge_summary = bridge.model_dump(include={'topo_id', 'name', 'theme', 'type'})

print("JSON representation:")
print(bridge_json)
```

### Database Integration Example

```python
import json
from typing import Dict, Any

def prepare_for_database(feature: TopoFeature) -> Dict[str, Any]:
    """Prepare feature for database storage."""
    data = feature.model_dump()
    
    # Convert geometry to WKT or GeoJSON string
    if 'geometry' in data:
        geometry = data.pop('geometry')
        data['geometry_json'] = json.dumps(geometry)
        
    # Handle UUID serialization
    if 'topo_id' in data:
        data['topo_id'] = str(data['topo_id'])
        
    return data

# Example usage
db_data = prepare_for_database(bridge)
print("Database-ready data:", db_data)
```

### Legacy Data Migration

```python
def migrate_legacy_bridge(legacy_data: dict) -> BridgeLine:
    """Convert legacy bridge data to new schema format."""
    
    # Map legacy fields to new schema
    return BridgeLine(
        topo_id=uuid4(),  # Generate new ID
        t50_fid=legacy_data.get('original_id'),  # Preserve legacy reference
        theme="transport", 
        type="bridge",
        
        # Convert legacy geometry format
        geometry=convert_legacy_geometry(legacy_data['geom']),
        
        # Map legacy attributes
        name=legacy_data.get('bridge_name'),
        name_ascii=legacy_data.get('bridge_name_ascii'),
        macronated=legacy_data.get('has_macrons', 'N'), 
        status=map_legacy_status(legacy_data.get('status')),
        
        # Bridge-specific mappings
        bridge_use=legacy_data.get('use_type', '').upper(),
        construction_type=legacy_data.get('material'),
        height=legacy_data.get('height_m'),
        
        # Metadata
        version=1,
        source="legacy_migration",
        create_date=datetime.now()
    )

def convert_legacy_geometry(legacy_geom) -> Geometry:
    """Convert legacy geometry format to GeoJSON-style Geometry."""
    # Implementation depends on legacy format
    # This is a placeholder
    return Geometry(
        type=GeometryType.LINESTRING,
        coordinates=[[0, 0], [1, 1]]  # Placeholder
    )

def map_legacy_status(legacy_status: str) -> str:
    """Map legacy status codes to new status values."""
    mapping = {
        'OP': 'operational',
        'UC': 'under_construction', 
        'AB': 'abandoned',
        'DM': 'demolished'
    }
    return mapping.get(legacy_status, 'unknown')

# Example migration
legacy_bridge_data = {
    'original_id': 123456,
    'bridge_name': 'Historic Bridge',
    'status': 'OP',
    'use_type': 'road',
    'material': 'stone',
    'height_m': 12.0,
    'geom': '...'  # Legacy geometry format
}

migrated_bridge = migrate_legacy_bridge(legacy_bridge_data)
print(f"Migrated bridge: {migrated_bridge.name}")
```

## Testing Examples

```python
import pytest
from pydantic import ValidationError

def test_bridge_creation():
    """Test valid bridge creation."""
    bridge = BridgeLine(
        topo_id=uuid4(),
        theme="transport",
        type="bridge",
        geometry=Geometry(
            type=GeometryType.LINESTRING,
            coordinates=[[0, 0], [1, 1]]
        ),
        version=1
    )
    
    assert bridge.theme == "transport"
    assert bridge.type == "bridge"
    assert bridge.version == 1

def test_bridge_validation():
    """Test bridge validation rules."""
    
    # Missing required fields should fail
    with pytest.raises(ValidationError):
        BridgeLine(theme="transport")
        
    # Invalid extension fields should fail  
    with pytest.raises(ValidationError):
        BridgeLine(
            topo_id=uuid4(),
            theme="transport",
            type="bridge", 
            geometry=Geometry(type=GeometryType.POINT, coordinates=[0, 0]),
            invalid_field="not allowed"
        )

def test_geometry_constraints():
    """Test geometry type constraints."""
    
    # LineString should be valid for bridges
    bridge = BridgeLine(
        topo_id=uuid4(),
        theme="transport",
        type="bridge",
        geometry=Geometry(
            type=GeometryType.LINESTRING, 
            coordinates=[[0, 0], [1, 1]]
        ),
        version=1
    )
    assert bridge.geometry.type == GeometryType.LINESTRING
    
    # Point geometry might be invalid for bridge lines
    # (depending on GeometryTypeConstraint implementation)

# Run tests
if __name__ == "__main__":
    test_bridge_creation()
    test_bridge_validation() 
    test_geometry_constraints()
    print("✓ All tests passed")
```

## Integration Patterns

### FastAPI Integration

```python
from fastapi import FastAPI, HTTPException
from typing import List

app = FastAPI(title="NZ Topographic API")

@app.post("/bridges/", response_model=BridgeLine)
async def create_bridge(bridge: BridgeLine):
    """Create a new bridge feature."""
    # Pydantic handles validation automatically
    # Save to database here
    return bridge

@app.get("/bridges/{bridge_id}", response_model=BridgeLine)  
async def get_bridge(bridge_id: str):
    """Get a bridge by ID."""
    # Load from database here
    # Return BridgeLine instance
    pass

@app.get("/bridges/", response_model=List[BridgeLine])
async def list_bridges(theme: str = "transport"):
    """List bridges by theme."""
    # Query database here
    # Return list of BridgeLine instances
    pass
```

### CLI Tool Integration

```python
import click
import json

@click.group()
def cli():
    """NZ Topographic Schema CLI tools."""
    pass

@cli.command()
@click.argument('input_file', type=click.File('r'))
@click.argument('output_file', type=click.File('w'))  
def validate(input_file, output_file):
    """Validate topographic data file."""
    try:
        data = json.load(input_file)
        
        # Validate based on feature type
        if data.get('type') == 'bridge':
            bridge = BridgeLine(**data)
            click.echo(f"✓ Valid bridge: {bridge.name}")
            
        output_file.write(bridge.model_dump_json(indent=2))
        
    except ValidationError as e:
        click.echo(f"✗ Validation failed: {e}", err=True)
        raise click.Abort()

if __name__ == '__main__':
    cli()
```

These examples show how to effectively use the NZ Topographic Schema in real-world applications. For more details, see the [API Reference](api/index.md).
