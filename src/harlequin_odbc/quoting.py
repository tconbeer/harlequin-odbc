from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyodbc

DEFAULT_QUOTE_CHAR = '"'
DEFAULT_CATALOG_SEPARATOR = "."

# SQLGetInfo(SQL_CATALOG_LOCATION) values, which pyodbc does not export
SQL_CL_END = 2

# SQLGetInfo reports one quote character, which opens and closes a name.
# SQL Server's brackets are the exception: they delimit a name whatever
# QUOTED_IDENTIFIER is set to, while its driver reports " or a space.
SQL_SERVER_DBMS_NAME = "Microsoft SQL Server"
CLOSING_QUOTE_CHARS = {"[": "]"}


def quote_identifier(name: str, quote_char: str = DEFAULT_QUOTE_CHAR) -> str:
    """The name as a delimited identifier, with its closing quote doubled inside."""
    closing_quote_char = CLOSING_QUOTE_CHARS.get(quote_char, quote_char)
    escaped_name = name.replace(closing_quote_char, closing_quote_char * 2)
    return f"{quote_char}{escaped_name}{closing_quote_char}"


def escape_search_pattern(name: str, escape_char: str) -> str:
    """The name as an ODBC search pattern that matches only itself.

    With no escape character, % and _ stay wildcards, and the pattern can
    match other names too.
    """
    if not escape_char:
        return name
    escaped_name = name
    for character in (escape_char, "%", "_"):
        escaped_name = escaped_name.replace(character, f"{escape_char}{character}")
    return escaped_name


def get_string_info(connection: pyodbc.Connection, info_type: int) -> str:
    """A string SQLGetInfo value, or "" if the driver does not report one."""
    try:
        value: Any = connection.getinfo(info_type)
    except Exception:
        return ""
    return value if isinstance(value, str) else ""


@dataclass(frozen=True)
class IdentifierQuoting:
    """How the connected DBMS delimits and qualifies identifiers."""

    quote_char: str = DEFAULT_QUOTE_CHAR
    catalog_separator: str = DEFAULT_CATALOG_SEPARATOR
    catalog_at_end: bool = False

    @classmethod
    def from_connection(cls, connection: pyodbc.Connection) -> IdentifierQuoting:
        """The rules the driver reports, with ANSI defaults for any it does not.

        A driver reports a space for its quote character when it does not
        support delimited identifiers, and "" for its separator when it does
        not support catalogs. SQL Server always gets brackets.
        """
        if get_string_info(connection, pyodbc.SQL_DBMS_NAME) == SQL_SERVER_DBMS_NAME:
            quote_char = "["
        else:
            quote_char = get_string_info(connection, pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)
        catalog_separator = get_string_info(
            connection, pyodbc.SQL_CATALOG_NAME_SEPARATOR
        )
        try:
            catalog_location = connection.getinfo(pyodbc.SQL_CATALOG_LOCATION)
        except Exception:
            catalog_location = None
        return cls(
            quote_char=(
                quote_char
                if len(quote_char) == 1 and not quote_char.isspace()
                else DEFAULT_QUOTE_CHAR
            ),
            catalog_separator=catalog_separator or DEFAULT_CATALOG_SEPARATOR,
            catalog_at_end=catalog_location == SQL_CL_END,
        )

    def identifier(self, name: str) -> str:
        """The name as a delimited identifier."""
        return quote_identifier(name, self.quote_char)

    def qualified(self, catalog: str | None, *names: str) -> str:
        """The dotted, delimited names, qualified by the catalog if there is one."""
        qualified_name = ".".join(self.identifier(name) for name in names)
        if catalog is None:
            return qualified_name
        quoted_catalog = self.identifier(catalog)
        if not names:
            return quoted_catalog
        if self.catalog_at_end:
            return f"{qualified_name}{self.catalog_separator}{quoted_catalog}"
        return f"{quoted_catalog}{self.catalog_separator}{qualified_name}"
