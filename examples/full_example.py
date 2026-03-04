"""
Duckling ORM — Full Example
============================

This example demonstrates all major features of Duckling,
showing how to use it similarly to Beanie ODM for MongoDB.
"""

import asyncio
import datetime
from typing import Annotated, List, Optional

from duckling import (
    Document,
    Indexed,
    IndexSpec,
    SortDirection,
    init_duckling,
    FindQuery,
    get_session,
)
from duckling.operators import And, Or, Not, In, Between, Like, Raw
from duckling.query import Count, Sum, Avg, Min, Max


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. Define your models (like Beanie Documents)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class User(Document):
    name: str
    email: Annotated[str, IndexSpec(unique=True)]   # Unique indexed field
    age: int = 0
    active: bool = True

    class Settings:
        table_name = "users"    # Custom table name (optional)


class Product(Document):
    name: str
    price: float
    category: Optional[str] = None
    tags: Optional[List[str]] = None    # Stored as JSON in DuckDB
    in_stock: bool = True

    class Settings:
        table_name = "products"


class Order(Document):
    user_id: int
    product_id: int
    quantity: int = 1
    total: float
    ordered_at: datetime.datetime = datetime.datetime.now()

    class Settings:
        table_name = "orders"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. Main async workflow
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    # ── Initialize ────────────────────────────
    # Just like Beanie's `init_beanie()`
    await init_duckling(
        database=":memory:",    # or "my_app.db" for persistent storage
        document_models=[User, Product, Order],
    )
    print("✓ Duckling initialized\n")

    # ── Insert ────────────────────────────────
    alice = User(name="Alice", email="alice@example.com", age=30)
    await alice.insert()
    print(f"Inserted: {alice} (id={alice.id})")

    bob = User(name="Bob", email="bob@example.com", age=25, active=False)
    await bob.insert()

    charlie = User(name="Charlie", email="charlie@example.com", age=35)
    await charlie.insert()

    # ── Insert Many (bulk) ────────────────────
    products = await Product.insert_many([
        Product(name="Laptop", price=999.99, category="electronics", tags=["tech", "portable"]),
        Product(name="Desk", price=299.50, category="furniture"),
        Product(name="Keyboard", price=79.99, category="electronics", in_stock=False),
        Product(name="Monitor", price=549.00, category="electronics"),
        Product(name="Chair", price=399.00, category="furniture", tags=["ergonomic"]),
    ])
    print(f"\nBulk inserted {len(products)} products")

    # ── Find One ──────────────────────────────
    user = await User.find_one(User.email == "alice@example.com")
    print(f"\nFind one: {user.name} (age={user.age})")

    # ── Get by ID ─────────────────────────────
    user = await User.get(alice.id)
    print(f"Get by id: {user.name}")

    # ── Find with conditions ──────────────────
    # Simple comparison (like Beanie)
    adults = await User.find(User.age >= 30).to_list()
    print(f"\nUsers aged ≥ 30: {[u.name for u in adults]}")

    # ── Chained queries ───────────────────────
    results = (
        await User.find(User.active == True)
        .sort("+name")          # ascending by name
        .limit(10)
        .to_list()
    )
    print(f"Active users (sorted): {[u.name for u in results]}")

    # ── Complex boolean queries ───────────────
    results = await User.find(
        (User.age > 20) & (User.active == True)
    ).to_list()
    print(f"Active and age > 20: {[u.name for u in results]}")

    results = await User.find(
        (User.name == "Alice") | (User.name == "Charlie")
    ).to_list()
    print(f"Alice or Charlie: {[u.name for u in results]}")

    # ── Operator functions ────────────────────
    results = await User.find(In(User.age, [25, 30])).to_list()
    print(f"\nAge in [25, 30]: {[u.name for u in results]}")

    results = await User.find(Between(User.age, 26, 36)).to_list()
    print(f"Age between 26-36: {[u.name for u in results]}")

    results = await User.find(Like(User.name, "A%")).to_list()
    print(f"Name starts with A: {[u.name for u in results]}")

    # ── FieldProxy helpers ────────────────────
    results = await User.find(User.name.startswith("C")).to_list()
    print(f"Starts with C: {[u.name for u in results]}")

    results = await User.find(User.name.contains("li")).to_list()
    print(f"Contains 'li': {[u.name for u in results]}")

    results = await User.find(User.age.is_in([25, 35])).to_list()
    print(f"Age is_in [25, 35]: {[u.name for u in results]}")

    # ── Sort with FieldProxy ──────────────────
    results = await User.find_all().sort(User.age.desc()).to_list()
    print(f"\nSorted by age desc: {[(u.name, u.age) for u in results]}")

    # ── Pagination (skip + limit) ─────────────
    page2 = await User.find_all().sort("+name").skip(1).limit(1).to_list()
    print(f"Page 2 (1 per page): {[u.name for u in page2]}")

    # ── Count & Exists ────────────────────────
    total = await User.count()
    print(f"\nTotal users: {total}")

    active_count = await User.find(User.active == True).count()
    print(f"Active users: {active_count}")

    has_alice = await User.find(User.name == "Alice").exists()
    print(f"Alice exists: {has_alice}")

    # ── Aggregation ───────────────────────────
    stats = await User.find_all().aggregate(
        avg_age=Avg("age"),
        max_age=Max("age"),
        min_age=Min("age"),
        total=Count(),
    )
    print(f"\nUser stats: {stats}")

    product_stats = await Product.find(
        Product.category == "electronics"
    ).aggregate(
        avg_price=Avg("price"),
        total=Count(),
        max_price=Max("price"),
    )
    print(f"Electronics stats: {product_stats}")

    # ── Update (save) ─────────────────────────
    alice.age = 31
    await alice.save()
    refreshed = await User.get(alice.id)
    print(f"\nUpdated Alice's age: {refreshed.age}")

    # ── Upsert (save without id inserts) ──────
    new_user = User(name="Diana", email="diana@example.com", age=28)
    await new_user.save()  # no id → inserts
    print(f"Upserted Diana: id={new_user.id}")

    # ── Refresh from DB ───────────────────────
    session = get_session()
    await session.async_execute(
        'UPDATE "users" SET "age" = ? WHERE "id" = ?', [99, alice.id]
    )
    await alice.refresh()
    print(f"Refreshed Alice from DB: age={alice.age}")

    # ── Delete ────────────────────────────────
    await bob.delete()
    remaining = await User.count()
    print(f"\nDeleted Bob. Remaining users: {remaining}")

    # ── Async iteration ───────────────────────
    print("\nAll products:")
    async for product in Product.find_all().sort("+price"):
        stock = "✓" if product.in_stock else "✗"
        print(f"  [{stock}] {product.name}: ${product.price:.2f} ({product.category})")

    # ── Transactions ──────────────────────────
    async with session.async_transaction():
        order = Order(
            user_id=alice.id,
            product_id=products[0].id,
            quantity=2,
            total=products[0].price * 2,
        )
        await order.insert()
    print(f"\nCreated order #{order.id} in transaction")

    # ── Delete all ────────────────────────────
    await Order.delete_all()
    print(f"Cleared all orders. Count: {await Order.count()}")

    print("\n✓ All examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
