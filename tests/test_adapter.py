import os
import sys
from typing import Generator

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import create_backend

from harlequin_odbc.adapter import (
    HarlequinOdbcAdapter,
    HarlequinOdbcConnection,
    HarlequinOdbcCursor,
)

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

CONN_STR = os.environ["ODBC_CONN_STR"]


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "odbc"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinOdbcAdapter


def test_connect() -> None:
    conn = HarlequinOdbcAdapter(conn_str=(CONN_STR,)).connect()
    assert isinstance(conn, HarlequinConnection)


def test_init_extra_kwargs() -> None:
    assert HarlequinOdbcAdapter(conn_str=(CONN_STR,), foo=1, bar="baz").connect()


def test_connect_raises_connection_error() -> None:
    with pytest.raises(HarlequinConnectionError):
        _ = HarlequinOdbcAdapter(conn_str=("foo",)).connect()


@pytest.fixture
def connection() -> Generator[HarlequinOdbcConnection, None, None]:
    conn = HarlequinOdbcAdapter(conn_str=(CONN_STR,)).connect()
    conn.execute("drop schema if exists test;")
    conn.execute("create schema test;")
    yield conn
    conn.execute("drop table if exists test.foo;")
    conn.execute("drop schema if exists test;")


def test_get_catalog(connection: HarlequinOdbcConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_execute_ddl(connection: HarlequinOdbcConnection) -> None:
    cur = connection.execute("create table test.foo (a int)")
    assert cur is None


def test_execute_select(connection: HarlequinOdbcConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinOdbcCursor)
    # assert cur.columns() == [("a", "##")]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_dupe_cols(connection: HarlequinOdbcConnection) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


def test_set_limit(connection: HarlequinOdbcConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: HarlequinOdbcConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("selec;")
