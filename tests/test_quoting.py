from __future__ import annotations

from typing import Any, cast

import pyodbc
import pytest

from harlequin_odbc.adapter import HarlequinOdbcConnection
from harlequin_odbc.quoting import (
    SQL_CL_END,
    IdentifierQuoting,
    escape_search_pattern,
    quote_identifier,
)


@pytest.mark.parametrize(
    "name,quote_char,expected",
    [
        ("foo", '"', '"foo"'),
        ("MiXed Case", '"', '"MiXed Case"'),
        ('a"b', '"', '"a""b"'),
        ('""', '"', '""""""'),
        ("a'b", '"', '"a\'b"'),
        ("a`b", '"', '"a`b"'),
        ("a]b", '"', '"a]b"'),
        ("a\\b", '"', '"a\\b"'),
        ("a.b", '"', '"a.b"'),
        ("a;b", '"', '"a;b"'),
        ("a\nb", '"', '"a\nb"'),
        ("a`b", "`", "`a``b`"),
        ('a"b', "`", '`a"b`'),
        ("a]b", "[", "[a]]b]"),
        ("a[b", "[", "[a[b]"),
        ('a"b', "[", '[a"b]'),
    ],
)
def test_quote_identifier(name: str, quote_char: str, expected: str) -> None:
    assert quote_identifier(name, quote_char) == expected


@pytest.mark.parametrize(
    "name,escape_char,expected",
    [
        ("plain", "\\", "plain"),
        ("a_b", "\\", "a\\_b"),
        ("100%", "\\", "100\\%"),
        ("a\\b", "\\", "a\\\\b"),
        ("\\_%", "\\", "\\\\\\_\\%"),
        ("a_b", "", "a_b"),
        ("a_b!", "!", "a!_b!!"),
    ],
)
def test_escape_search_pattern(name: str, escape_char: str, expected: str) -> None:
    assert escape_search_pattern(name, escape_char) == expected


def test_qualified_catalog_at_start() -> None:
    quoting = IdentifierQuoting()

    assert quoting.qualified("d.b", 's"ch', "t]b") == '"d.b"."s""ch"."t]b"'
    assert quoting.qualified(None, 's"ch', "t]b") == '"s""ch"."t]b"'
    assert quoting.qualified("d.b") == '"d.b"'


def test_qualified_catalog_at_end() -> None:
    quoting = IdentifierQuoting(
        quote_char="`", catalog_separator="@", catalog_at_end=True
    )

    assert quoting.qualified("lnk", "sch", "t`b") == "`sch`.`t``b`@`lnk`"
    assert quoting.qualified("lnk") == "`lnk`"


class FakeConnection:
    def __init__(self, info: dict[int, Any]) -> None:
        self.info = info

    def getinfo(self, info_type: int) -> Any:
        value = self.info[info_type]
        if isinstance(value, Exception):
            raise value
        return value


@pytest.mark.parametrize(
    "info,expected",
    [
        (
            {
                pyodbc.SQL_DBMS_NAME: "MySQL",
                pyodbc.SQL_IDENTIFIER_QUOTE_CHAR: "`",
                pyodbc.SQL_CATALOG_NAME_SEPARATOR: ".",
                pyodbc.SQL_CATALOG_LOCATION: 1,
            },
            IdentifierQuoting(quote_char="`"),
        ),
        (
            {
                pyodbc.SQL_DBMS_NAME: "Some DBMS",
                pyodbc.SQL_IDENTIFIER_QUOTE_CHAR: "[",
                pyodbc.SQL_CATALOG_NAME_SEPARATOR: "@",
                pyodbc.SQL_CATALOG_LOCATION: SQL_CL_END,
            },
            IdentifierQuoting(
                quote_char="[", catalog_separator="@", catalog_at_end=True
            ),
        ),
        (
            # a driver that supports neither delimited identifiers nor catalogs
            {
                pyodbc.SQL_DBMS_NAME: "Some DBMS",
                pyodbc.SQL_IDENTIFIER_QUOTE_CHAR: " ",
                pyodbc.SQL_CATALOG_NAME_SEPARATOR: "",
                pyodbc.SQL_CATALOG_LOCATION: 0,
            },
            IdentifierQuoting(),
        ),
        (
            # SQL Server's driver reports a space when QuotedId=No
            {
                pyodbc.SQL_DBMS_NAME: "Microsoft SQL Server",
                pyodbc.SQL_IDENTIFIER_QUOTE_CHAR: " ",
                pyodbc.SQL_CATALOG_NAME_SEPARATOR: ".",
                pyodbc.SQL_CATALOG_LOCATION: 1,
            },
            IdentifierQuoting(quote_char="["),
        ),
        (
            {
                pyodbc.SQL_DBMS_NAME: pyodbc.Error("HY096"),
                pyodbc.SQL_IDENTIFIER_QUOTE_CHAR: pyodbc.Error("HY096"),
                pyodbc.SQL_CATALOG_NAME_SEPARATOR: pyodbc.Error("HY096"),
                pyodbc.SQL_CATALOG_LOCATION: pyodbc.Error("HY096"),
            },
            IdentifierQuoting(),
        ),
    ],
)
def test_from_connection(info: dict[int, Any], expected: IdentifierQuoting) -> None:
    fake_connection = cast(pyodbc.Connection, FakeConnection(info))

    assert IdentifierQuoting.from_connection(fake_connection) == expected


def test_from_sql_server_connection(
    connection: HarlequinOdbcConnection,
    connection_without_quoted_identifiers: HarlequinOdbcConnection,
) -> None:
    expected_quoting = IdentifierQuoting(
        quote_char="[", catalog_separator=".", catalog_at_end=False
    )

    assert connection.identifier_quoting == expected_quoting
    assert connection_without_quoted_identifiers.identifier_quoting == (
        expected_quoting
    )
