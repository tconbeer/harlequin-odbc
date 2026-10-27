from __future__ import annotations

from typing import Any, Callable

import pyodbc
import pytest
from harlequin.driver import HarlequinDriver

from harlequin_odbc.adapter import HarlequinOdbcConnection, HarlequinOdbcCursor
from harlequin_odbc.catalog import (
    DatabaseCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
    TableCatalogItem,
    ViewCatalogItem,
)
from harlequin_odbc.interactions import (
    execute_drop_database_statement,
    execute_drop_table_statement,
    execute_drop_view_statement,
    execute_use_statement,
    insert_columns_at_cursor,
    show_select_star,
)

SCHEMA_NAME = "Sch \"q]'.;\\ `x"
TABLE_NAMES = [
    "MiXed Case",
    'dq"uote',
    "sq'uote",
    "back`tick",
    "close]bracket",
    "open[bracket",
    "back\\slash",
    "dot.ted",
    "semi;colon",
    "new\nline",
    # pairs an unescaped search pattern cannot tell apart
    "a_b",
    "axb",
    "p%q",
    "pXq",
]
VIEW_NAME = "vi\"ew]'.;"


class RecordingDriver(HarlequinDriver):
    def __init__(self) -> None:
        self.inserted_text: list[str] = []
        self.notifications: list[tuple[str, str]] = []

    def insert_text_at_selection(self, text: str) -> None:
        self.inserted_text.append(text)

    def insert_text_in_new_buffer(self, text: str) -> None:
        self.inserted_text.append(text)

    def confirm_and_execute(
        self, callback: Callable[[], None], instructions: str = "Are you sure?"
    ) -> None:
        callback()

    def notify(self, message: str, severity: Any = "information") -> None:
        self.notifications.append((message, severity))

    def refresh_catalog(self) -> None:
        pass


def _execute_quoted(cur: pyodbc.Cursor, template: str, *names: str) -> None:
    """Runs DDL with the dotted names, each quoted by QUOTENAME, in place of {}."""
    quoted_names = " + N'.' + ".join("quotename(?)" for _ in names)
    before, after = template.split("{}")
    cur.execute(
        f"declare @s nvarchar(max) = N'{before}' + {quoted_names} + N'{after}'; "
        "exec(@s)",
        *names,
    )


@pytest.fixture
def connection_with_quoted_names(
    connection: HarlequinOdbcConnection,
) -> HarlequinOdbcConnection:
    cur = connection.conn.cursor()
    _execute_quoted(cur, "create schema {}", SCHEMA_NAME)
    for table_name in TABLE_NAMES:
        _execute_quoted(
            cur,
            "create table {} (marker nvarchar(200), filler int)",
            SCHEMA_NAME,
            table_name,
        )
        cur.execute(
            "declare @s nvarchar(max) = N'insert into ' + quotename(?) + N'.' "
            "+ quotename(?) + N' (marker) values (@marker)'; "
            "exec sp_executesql @s, N'@marker nvarchar(200)', @marker = ?",
            SCHEMA_NAME,
            table_name,
            table_name,
        )
    _execute_quoted(cur, "create view {} as select 1 as marker", SCHEMA_NAME, VIEW_NAME)
    # the connection fixture drops the whole database afterward
    return connection


def _rows(cur: HarlequinOdbcCursor | pyodbc.Cursor) -> list[tuple[Any, ...]]:
    """The rows a cursor returns, as plain tuples."""
    return [tuple(row) for row in cur.fetchall()]


def _relation_items(connection: HarlequinOdbcConnection) -> list[RelationCatalogItem]:
    catalog = connection.get_catalog()
    [test_db_item] = [item for item in catalog.items if item.label == "test"]
    [schema_item] = [
        item for item in test_db_item.children if item.label == SCHEMA_NAME
    ]
    assert isinstance(schema_item, SchemaCatalogItem)
    relation_items = [
        item for item in schema_item.children if isinstance(item, RelationCatalogItem)
    ]
    assert len(relation_items) == len(schema_item.children)
    return relation_items


def _object_exists(
    connection: HarlequinOdbcConnection, schema_name: str, relation_name: str
) -> bool:
    cur = connection.aux_conn.cursor()
    cur.execute(
        "select count(*) from test.sys.objects o "
        "join test.sys.schemas s on o.schema_id = s.schema_id "
        "where s.name = ? and o.name = ?",
        schema_name,
        relation_name,
    )
    row = cur.fetchone()
    assert row is not None
    return bool(row[0])


def test_catalog_loads_names_that_need_quoting(
    connection_with_quoted_names: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_quoted_names

    relation_items = _relation_items(conn)

    assert {item.label for item in relation_items} == {*TABLE_NAMES, VIEW_NAME}
    for item in relation_items:
        if item.label == VIEW_NAME:
            assert isinstance(item, ViewCatalogItem)
            continue
        assert isinstance(item, TableCatalogItem)
        columns = item.fetch_children()
        assert [column.label for column in columns] == ["marker", "filler"]
        for identifier in (item.qualified_identifier, item.query_name):
            cur = conn.execute(f"select marker from {identifier}")
            assert cur is not None
            assert _rows(cur) == [(item.label,)]
        [marker_column, _] = columns
        cur = conn.execute(
            f"select {marker_column.qualified_identifier} "
            f"from {item.qualified_identifier}"
        )
        assert cur is not None
        assert _rows(cur) == [(item.label,)]


def test_show_select_star(
    connection_with_quoted_names: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_quoted_names

    for item in _relation_items(conn):
        driver = RecordingDriver()
        show_select_star(item, driver)
        [select_star] = driver.inserted_text
        cur = conn.execute(select_star)
        assert cur is not None
        if item.label == VIEW_NAME:
            assert _rows(cur) == [(1,)]
        else:
            assert _rows(cur) == [(item.label, None)]


def test_insert_columns_at_cursor(
    connection_with_quoted_names: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_quoted_names

    for item in _relation_items(conn):
        if item.label == VIEW_NAME:
            continue
        driver = RecordingDriver()
        insert_columns_at_cursor(item, driver)
        [column_list] = driver.inserted_text
        cur = conn.execute(f"select {column_list} from {item.query_name}")
        assert cur is not None
        assert _rows(cur) == [(item.label, None)]


def test_drop_table_and_view(
    connection_with_quoted_names: HarlequinOdbcConnection,
) -> None:
    conn = connection_with_quoted_names
    remaining_names = {*TABLE_NAMES, VIEW_NAME}

    for item in _relation_items(conn):
        driver = RecordingDriver()
        if isinstance(item, ViewCatalogItem):
            execute_drop_view_statement(item, driver)
        else:
            execute_drop_table_statement(item, driver)
        assert all(severity != "error" for _, severity in driver.notifications)
        remaining_names.remove(item.label)
        assert not _object_exists(conn, SCHEMA_NAME, item.label)
        for name in remaining_names:
            assert _object_exists(conn, SCHEMA_NAME, name)


def _current_database(connection: HarlequinOdbcConnection) -> str:
    cur = connection.execute("select db_name()")
    assert cur is not None
    [(database_name,)] = _rows(cur)
    assert isinstance(database_name, str)
    return database_name


def test_use_database(
    connection: HarlequinOdbcConnection, quoted_database_name: str
) -> None:
    item = DatabaseCatalogItem.from_label(
        label=quoted_database_name, connection=connection
    )
    driver = RecordingDriver()

    execute_use_statement(item, driver)

    assert _current_database(connection) == quoted_database_name
    connection.execute("use test")


def test_drop_database(
    connection: HarlequinOdbcConnection, quoted_database_name: str
) -> None:
    item = DatabaseCatalogItem.from_label(
        label=quoted_database_name, connection=connection
    )
    driver = RecordingDriver()

    execute_drop_database_statement(item, driver)

    cur = connection.aux_conn.cursor()
    cur.execute("select db_id(?)", quoted_database_name)
    assert _rows(cur) == [(None,)]


@pytest.mark.parametrize(
    "pattern_escape",
    [
        # a driver with no escape character, whose patterns over-match
        "",
        # a driver that reports an escape character it does not honor
        "!",
    ],
)
def test_columns_belong_to_their_relation(
    connection_with_quoted_names: HarlequinOdbcConnection, pattern_escape: str
) -> None:
    conn = connection_with_quoted_names
    conn._pattern_escape = pattern_escape

    relation_items = {item.label: item for item in _relation_items(conn)}

    for name in ("a_b", "p%q"):
        columns = relation_items[name].fetch_children()
        assert [column.label for column in columns] == ["marker", "filler"]


def test_interactions_without_quoted_identifiers(
    connection_with_quoted_names: HarlequinOdbcConnection,
    connection_without_quoted_identifiers: HarlequinOdbcConnection,
    quoted_database_name: str,
) -> None:
    conn = connection_without_quoted_identifiers
    # with QUOTED_IDENTIFIER off, a double-quoted name is a string
    cur = conn.execute('select "not an identifier"')
    assert cur is not None
    assert _rows(cur) == [("not an identifier",)]

    for item in _relation_items(conn):
        driver = RecordingDriver()
        show_select_star(item, driver)
        [select_star] = driver.inserted_text
        cur = conn.execute(select_star)
        assert cur is not None
        assert len(_rows(cur)) == 1

    database_item = DatabaseCatalogItem.from_label(
        label=quoted_database_name, connection=conn
    )
    execute_use_statement(database_item, RecordingDriver())
    assert _current_database(conn) == quoted_database_name
    conn.execute("use test")
