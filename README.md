# 🦆 Duckling

**A Beanie-inspired ORM for DuckDB — async-first, Pydantic-powered.**

Duckling brings the elegant, developer-friendly API of [Beanie](https://github.com/BeanieODM/beanie) (MongoDB ODM) to [DuckDB](https://duckdb.org/) — the fast, in-process analytical database. Define your models with Pydantic, query with Pythonic expressions, and enjoy both async and sync APIs.

## Installation

```bash
pip install duckling
# or from source:
pip install -e .
```

**Requirements:** Python ≥ 3.10, `duckdb >= 0.9`, `pydantic >= 2.0`

---

## Quick Start

```python
import asyncio
from typing import Annotated, Optional
from duckling import Document, IndexSpec, init_duckling

class User(Document):
    name: str
    email: Annotated[str, IndexSpec(unique=True)]
    age: int = 0

    class Settings:
        table_name = "users"

async def main():
    await init_duckling(database=":memory:", document_models=[User])

    # Insert
    alice = User(name="Alice", email="alice@example.com", age=30)
    await alice.insert()

    # Query
    users = await User.find(User.age > 25).sort("+name").limit(10).to_list()

    # Update
    alice.age = 31
    await alice.save()

    # Delete
    await alice.delete()

asyncio.run(main())
```

---

## API Reference

### Initialization

```python
from duckling import init_duckling, init_duckling_sync

# Async
await init_duckling(
    database=":memory:",          # or "path/to/file.db"
    document_models=[User, Product],
    recreate_tables=False,        # drop & recreate tables
)

# Sync
init_duckling_sync(database="app.db", document_models=[User])
```

### Defining Models

Duckling models are Pydantic `BaseModel` subclasses with an auto-generated `id` primary key:

```python
from duckling import Document, IndexSpec
from typing import Annotated, Optional, List
import datetime

class Product(Document):
    name: str
    price: float
    category: Optional[str] = None
    tags: Optional[List[str]] = None       # stored as JSON
    created_at: datetime.datetime = datetime.datetime.now()
    in_stock: bool = True

    class Settings:
        table_name = "products"   # optional, auto-generated from class name
```

**Supported types:** `str`, `int`, `float`, `bool`, `bytes`, `datetime.date`, `datetime.datetime`, `datetime.time`, `uuid.UUID`, `Optional[T]`, `List[T]` (→ JSON), `dict` (→ JSON), nested Pydantic models (→ JSON), `Enum`.

### Indexed Fields

```python
from duckling import IndexSpec
from typing import Annotated

class User(Document):
    email: Annotated[str, IndexSpec(unique=True)]   # unique index
    age: Annotated[int, IndexSpec()]                 # regular index
```

### CRUD Operations

Every method has an async version (default) and a `_sync` variant:

| Async | Sync | Description |
|---|---|---|
| `await doc.insert()` | `doc.insert_sync()` | Insert a new document |
| `await doc.save()` | `doc.save_sync()` | Upsert (insert or update) |
| `await doc.delete()` | `doc.delete_sync()` | Delete this document |
| `await doc.refresh()` | — | Reload from database |
| `await Model.insert_many([...])` | `Model.insert_many_sync([...])` | Bulk insert |
| `await Model.delete_all()` | `Model.delete_all_sync()` | Delete all rows |
| `await Model.get(id)` | `Model.get_sync(id)` | Fetch by primary key |
| `await Model.count()` | `Model.count_sync()` | Count all rows |

### Queries

Duckling's query interface mirrors Beanie's fluent API:

```python
# Find with conditions
users = await User.find(User.age > 25).to_list()
users = await User.find(User.age > 25, User.active == True).to_list()

# Find one
user = await User.find_one(User.email == "alice@example.com")

# Find all
all_users = await User.find_all().to_list()

# Chaining
results = (
    await User.find(User.active == True)
    .find(User.age >= 18)          # additional conditions (AND)
    .sort("+name")                  # ascending
    .sort("-age")                   # descending
    .skip(10)                       # offset
    .limit(20)                      # limit
    .to_list()
)

# Count & exists
count = await User.find(User.age > 30).count()
has_any = await User.find(User.name == "Alice").exists()

# Async iteration
async for user in User.find(User.active == True).sort("+name"):
    print(user.name)
```

### Query Expressions

Use Pythonic operators directly on model fields:

```python
# Comparison operators
User.age == 30          User.age != 30
User.age > 25           User.age >= 25
User.age < 40           User.age <= 40

# Boolean combinators
(User.age > 25) & (User.active == True)    # AND
(User.name == "A") | (User.name == "B")    # OR
~(User.active == True)                      # NOT

# FieldProxy helper methods
User.name.startswith("Ali")     # LIKE 'Ali%'
User.name.endswith("son")       # LIKE '%son'
User.name.contains("lic")       # LIKE '%lic%'
User.name.like("A%e")           # LIKE 'A%e'
User.name.ilike("alice")        # ILIKE (case-insensitive)
User.age.is_in([25, 30, 35])    # IN (25, 30, 35)
User.age.not_in([0, 99])        # NOT IN
User.age.between(18, 65)        # BETWEEN 18 AND 65

# Sort helpers
User.name.asc()     # → ("name", ASCENDING)
User.name.desc()    # → ("name", DESCENDING)
```

### Operator Functions

For more complex queries, use the operator functions:

```python
from duckling.operators import And, Or, Not, In, NotIn, Between, Like, ILike, Raw

await User.find(In(User.age, [25, 30, 35])).to_list()
await User.find(Between(User.age, 18, 65)).to_list()
await User.find(Like(User.name, "%smith%")).to_list()

# Combine
await User.find(
    And(
        User.active == True,
        Or(User.city == "NYC", User.city == "LA"),
        Not(User.age < 18),
    )
).to_list()

# Raw SQL escape hatch
await User.find(Raw('"age" % 2 = 0')).to_list()
```

### Aggregation

```python
from duckling.query import Count, Sum, Avg, Min, Max, CountDistinct

stats = await User.find(User.active == True).aggregate(
    total=Count(),
    avg_age=Avg("age"),
    max_age=Max("age"),
    min_age=Min("age"),
    sum_age=Sum("age"),
    unique_names=CountDistinct("name"),
)
print(stats)  # {'total': 42, 'avg_age': 31.5, ...}
```

### Sort Syntax

```python
# String syntax
.sort("+name")          # ascending
.sort("-age")           # descending
.sort("+name", "-age")  # multi-column

# Tuple syntax
.sort(("name", SortDirection.ASCENDING))

# FieldProxy syntax
.sort(User.name.asc(), User.age.desc())
```

### Transactions

```python
session = get_session()

# Async
async with session.async_transaction():
    await user.insert()
    await order.insert()

# Sync
with session.transaction():
    user.insert_sync()
    order.insert_sync()
```

### Raw SQL Escape Hatch

```python
from duckling import get_session

session = get_session()

# Async
rows = await session.async_fetchall("SELECT * FROM users WHERE age > ?", [25])

# Get pandas DataFrame
df = await session.async_fetchdf("SELECT name, age FROM users")

# Sync
rows = session.fetchall("SELECT count(*) FROM users")
```

---

## Beanie → Duckling Comparison

| Beanie (MongoDB) | Duckling (DuckDB) |
|---|---|
| `init_beanie(database, models)` | `await init_duckling(database, models)` |
| `class User(Document)` | `class User(Document)` |
| `await user.insert()` | `await user.insert()` |
| `await user.save()` | `await user.save()` |
| `await User.find(cond).to_list()` | `await User.find(cond).to_list()` |
| `await User.find_one(cond)` | `await User.find_one(cond)` |
| `User.name == "Alice"` | `User.name == "Alice"` |
| `In(User.age, [...])` | `In(User.age, [...])` |
| `await User.find().sort("+name")` | `await User.find().sort("+name")` |
| Settings class | Settings class |
| `Indexed(str, unique=True)` | `Annotated[str, IndexSpec(unique=True)]` |

---

## Project Structure

```
duckling/
├── __init__.py         # Public exports
├── connection.py       # DuckDB session management
├── document.py         # Document base class (the core)
├── fields.py           # FieldProxy, Indexed, Expression types
├── init.py             # init_duckling() / init_duckling_sync()
├── operators.py        # And, Or, In, Between, Like, etc.
├── query.py            # FindQuery builder + aggregation
└── exceptions.py       # Custom exceptions
```

## License

MIT
