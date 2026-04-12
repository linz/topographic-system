# Pydantic Data Validation

This document explains how to use **Pydantic** to validate, parse, and manage structured data in Python using type hints.

Pydantic is widely used for:
- Input validation (APIs, CLIs, config files)
- Data parsing and coercion
- Enforcing schemas and constraints
- FastAPI request and response models

---

## Requirements

- Python **3.8+**
- Pydantic installed

```bash
pip install pydantic
```

---

## Basic Usage

Create a Pydantic model by subclassing `BaseModel` and defining fields with type hints.

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str
```

```python
user = User(id="1", name="Alice", email="alice@example.com")
print(user.id)        # 1
print(type(user.id))  # <class 'int'>
```

Pydantic automatically:
- Validates input data
- Converts compatible types
- Raises clear validation errors

---

## Validation Errors

Invalid data raises a `ValidationError`.

```python
from pydantic import ValidationError

try:
    User(id="abc", name="Alice", email="alice@example.com")
except ValidationError as e:
    print(e)
```

---

## Field Constraints

Use `Field` to add validation rules and metadata.

```python
from pydantic import BaseModel, Field

class Product(BaseModel):
    name: str = Field(..., min_length=3)
    price: float = Field(..., gt=0)
    quantity: int = Field(default=1, ge=0)
```

Common constraints include:
- `min_length`, `max_length`
- `gt`, `ge`, `lt`, `le`
- `regex`

---

## Optional Fields

```python
from typing import Optional
from pydantic import BaseModel

class Profile(BaseModel):
    username: str
    bio: Optional[str] = None
```

---

## Custom Validators (Pydantic v2)

```python
from pydantic import BaseModel, field_validator

class User(BaseModel):
    name: str
    email: str

    @field_validator("email")
    @classmethod
    def validate_email(cls, value):
        if "@" not in value:
            raise ValueError("Invalid email address")
        return value
```

---

## Parsing Data

### From a dictionary

```python
User.model_validate({"id": 1, "name": "Bob", "email": "bob@example.com"})
```

### From JSON

```python
User.model_validate_json('{"id":1,"name":"Bob","email":"bob@example.com"}')
```

---

## Exporting Data

```python
user.model_dump()
user.model_dump_json()
```

---

## Nested Models

```python
class Address(BaseModel):
    city: str
    country: str

class User(BaseModel):
    name: str
    address: Address
```

```python
User(name="Alice", address={"city": "Wellington", "country": "NZ"})
```

---

## Strict Types

```python
from pydantic import StrictInt

class Example(BaseModel):
    count: StrictInt
```

---

## Settings Models

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str
    debug: bool = False
```

---

## When to Use Pydantic

Use Pydantic when you need:
- Strong validation guarantees
- Typed schemas
- Safe parsing of untrusted data
- FastAPI integrations

---

## Resources

- https://docs.pydantic.dev/
- https://fastapi.tiangolo.com/
