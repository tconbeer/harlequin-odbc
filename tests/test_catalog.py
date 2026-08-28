from typing import Generator

import pytest
from harlequin.catalog import CatalogSearchResult, InteractiveCatalogItem
from harlequin.exception import HarlequinQueryError

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


@pytest.fixture
def connection_with_search_objects(
    connection: HarlequinOdbcConnection,
) -> Generator[HarlequinOdbcConnection, None, None]:
    connection.execute("create schema srch")
    connection.execute("select 1 as customer_id, '2' as amount into srch.orders")
    connection.execute("create view srch.orders_vw as select * from srch.orders")
    # a pair that only an escaped pattern tells apart, since _ matches any
    # single character in an ODBC search pattern
    connection.execute("select 1 as a into srch.a_b")
    connection.execute("select 1 as a into srch.axb")

    yield connection

    connection.execute("drop view srch.orders_vw")
    connection.execute("drop table srch.orders")
    connection.execute("drop table srch.a_b")
    connection.execute("drop table srch.axb")
    connection.execute("drop schema srch")


def _in_test_schema(results: list[CatalogSearchResult]) -> list[CatalogSearchResult]:
    """The results under test.srch, without the server's other databases."""
    return [result for result in results if result.parents[:2] == ("test", "srch")]


def test_search_catalog_relations(
    connection_with_search_objects: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_search_objects

    results = _in_test_schema(conn.search_catalog("orders", kind="relations"))

    assert {result.item.label for result in results} == {"orders", "orders_vw"}
    assert all(result.parents == ("test", "srch") for result in results)
    assert all(isinstance(result.item, RelationCatalogItem) for result in results)

    [orders] = [result.item for result in results if result.item.label == "orders"]
    assert isinstance(orders, TableCatalogItem)
    assert orders.query_name == '"srch"."orders"'
    assert orders.qualified_identifier == '"test"."srch"."orders"'

    [view] = [result.item for result in results if result.item.label == "orders_vw"]
    assert isinstance(view, ViewCatalogItem)


def test_search_catalog_columns(
    connection_with_search_objects: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_search_objects

    results = _in_test_schema(conn.search_catalog("customer_id", kind="columns"))

    assert all(isinstance(result.item, ColumnCatalogItem) for result in results)
    assert {(result.parents, result.item.label) for result in results} == {
        (("test", "srch", "orders"), "customer_id"),
        (("test", "srch", "orders_vw"), "customer_id"),
    }

    [column] = [result.item for result in results if result.parents[-1] == "orders"]
    assert column.query_name == '"customer_id"'
    assert column.qualified_identifier == '"test"."srch"."orders"."customer_id"'
    assert column.type_label == "int"

    # the amount column does not match, and its relation is not a hit, either
    assert not _in_test_schema(conn.search_catalog("amount", kind="relations"))


def test_search_catalog_all(
    connection_with_search_objects: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_search_objects

    results = conn.search_catalog("srch")

    [schema_result] = [
        result for result in results if isinstance(result.item, SchemaCatalogItem)
    ]
    assert schema_result.item.label == "srch"
    assert schema_result.parents == ("test",)

    [db_result] = [
        result
        for result in conn.search_catalog("test")
        if isinstance(result.item, DatabaseCatalogItem)
    ]
    assert db_result.item.label == "test"
    assert db_result.parents == ()

    # "all" is every level, so a relation and its columns both come back
    all_results = _in_test_schema(conn.search_catalog("customer_id"))
    assert {type(result.item) for result in all_results} == {ColumnCatalogItem}

    orders_results = _in_test_schema(conn.search_catalog("orders_vw"))
    assert {result.item.label for result in orders_results} == {"orders_vw"}


def test_search_catalog_is_case_insensitive(
    connection_with_search_objects: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_search_objects

    upper = _in_test_schema(conn.search_catalog("ORDERS"))
    lower = _in_test_schema(conn.search_catalog("orders"))

    assert [result.item.label for result in upper] == [
        result.item.label for result in lower
    ]
    assert {result.item.label for result in upper} >= {"orders", "orders_vw"}
    assert {
        result.item.label
        for result in _in_test_schema(conn.search_catalog("CUSTOMER_ID"))
    } == {"customer_id"}


def test_search_catalog_escapes_pattern_metacharacters(
    connection_with_search_objects: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_search_objects

    results = _in_test_schema(conn.search_catalog("a_b"))

    assert {result.item.label for result in results} == {"a_b"}
    assert not _in_test_schema(conn.search_catalog("%"))


def test_search_catalog_no_matches(
    connection_with_search_objects: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_search_objects

    assert conn.search_catalog("no_such_object_anywhere") == []


def test_search_catalog_raises_on_driver_error(
    connection: HarlequinOdbcConnection,
) -> None:
    connection.close()

    with pytest.raises(HarlequinQueryError):
        connection.search_catalog("orders")
