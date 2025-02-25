from typing import Generator

import pytest
from harlequin.catalog import InteractiveCatalogItem

from harlequin_odbc.adapter import HarlequinOdbcConnection
from harlequin_odbc.catalog import (
    ColumnCatalogItem,
    DatabaseCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
    TableCatalogItem,
    ViewCatalogItem,
)


@pytest.fixture
def connection_with_objects(
    connection: HarlequinOdbcConnection,
) -> Generator[HarlequinOdbcConnection, None, None]:
    connection.execute("create schema one")
    connection.execute("select 1 as a, '2' as b into one.foo")
    connection.execute("select 1 as a, '2' as b into one.bar")
    connection.execute("select 1 as a, '2' as b into one.baz")
    connection.execute("create schema two")
    connection.execute("create view two.qux as select * from one.foo")
    connection.execute("create schema three")

    yield connection

    connection.execute("drop table one.foo")
    connection.execute("drop table one.bar")
    connection.execute("drop table one.baz")
    connection.execute("drop schema one")
    connection.execute("drop view two.qux")
    connection.execute("drop schema two")
    connection.execute("drop schema three")


def test_catalog(connection_with_objects: HarlequinOdbcConnection) -> None:
    conn = connection_with_objects

    catalog = conn.get_catalog()

    # at least two databases, postgres and test
    assert len(catalog.items) >= 2

    [test_db_item] = filter(lambda item: item.label == "test", catalog.items)
    assert isinstance(test_db_item, InteractiveCatalogItem)
    assert isinstance(test_db_item, DatabaseCatalogItem)
    assert test_db_item.children
    assert test_db_item.loaded

    schema_items = test_db_item.children
    assert all(isinstance(item, SchemaCatalogItem) for item in schema_items)

    [schema_one_item] = filter(lambda item: item.label == "one", schema_items)
    assert isinstance(schema_one_item, SchemaCatalogItem)
    assert schema_one_item.children
    assert schema_one_item.loaded

    table_items = schema_one_item.children
    assert all(isinstance(item, RelationCatalogItem) for item in table_items)

    [foo_item] = filter(lambda item: item.label == "foo", table_items)
    assert isinstance(foo_item, TableCatalogItem)
    assert not foo_item.children
    assert not foo_item.loaded

    foo_column_items = foo_item.fetch_children()
    assert all(isinstance(item, ColumnCatalogItem) for item in foo_column_items)

    [schema_two_item] = filter(lambda item: item.label == "two", schema_items)
    assert isinstance(schema_two_item, SchemaCatalogItem)
    assert schema_two_item.children
    assert schema_two_item.loaded

    view_items = schema_two_item.children
    assert all(isinstance(item, ViewCatalogItem) for item in view_items)

    [qux_item] = filter(lambda item: item.label == "qux", view_items)
    assert isinstance(qux_item, ViewCatalogItem)
    assert not qux_item.children
    assert not qux_item.loaded

    qux_column_items = qux_item.fetch_children()
    assert all(isinstance(item, ColumnCatalogItem) for item in qux_column_items)

    assert [item.label for item in foo_column_items] == [
        item.label for item in qux_column_items
    ]

    # ensure calling fetch_children on cols doesn't raise
    children_items = foo_column_items[0].fetch_children()
    assert not children_items

    # empty schemas don't appear in the catalog
    schema_three_items = list(filter(lambda item: item.label == "three", schema_items))
    assert not schema_three_items
