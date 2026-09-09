"""
Tests that rows are mapped onto fields by name-driven projection, not by the
table's physical column order.

`_from_row` zips a result row against the model's field order, so any SELECT
that does not spell out its columns will silently map values into the wrong
fields whenever the table's on-disk order differs from the model's.
"""

import pytest

from duckling import get_session

from .models import Product

# Deliberately not the model's field order (id, name, price, category, tags,
# in_stock) — this is what an externally created table can look like.
SHUFFLED_DDL = """
CREATE TABLE "products" (
  "in_stock" BOOLEAN,
  "name" VARCHAR,
  "tags" JSON,
  "price" DOUBLE,
  "category" VARCHAR,
  "id" BIGINT PRIMARY KEY
)
"""

INSERT_ROW = """
INSERT INTO "products" ("in_stock", "name", "tags", "price", "category", "id")
VALUES (true, 'Widget', NULL, 9.5, 'tools', 1)
"""


@pytest.fixture
def shuffled_products(sync_db):
    """Replace the generated products table with one in a different order."""
    session = get_session()
    session.execute('DROP TABLE "products"')
    session.execute(SHUFFLED_DDL)
    session.execute(INSERT_ROW)
    return sync_db


class TestPhysicalColumnOrder:
    def test_get_maps_by_name(self, shuffled_products):
        product = Product.get_sync(1)

        assert product is not None
        assert product.id == 1
        assert product.name == "Widget"
        assert product.price == 9.5
        assert product.category == "tools"
        assert product.in_stock is True

    def test_find_maps_by_name(self, shuffled_products):
        products = Product.find_all().to_list_sync()

        assert len(products) == 1
        assert products[0].name == "Widget"
        assert products[0].price == 9.5

    def test_filtered_find_maps_by_name(self, shuffled_products):
        products = Product.find(Product.category == "tools").to_list_sync()

        assert len(products) == 1
        assert products[0].name == "Widget"

    @pytest.mark.asyncio
    async def test_async_get_maps_by_name(self, shuffled_products):
        product = await Product.get(1)

        assert product is not None
        assert product.name == "Widget"
        assert product.price == 9.5
