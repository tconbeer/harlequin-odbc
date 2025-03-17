from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any, Sequence

import pyodbc
from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import (
    HarlequinConfigError,
    HarlequinConnectionError,
    HarlequinQueryError,
)
from textual_fastdatatable.backend import AutoBackendType

from harlequin_odbc.catalog import (
    DatabaseCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
)
from harlequin_odbc.cli_options import ODBC_OPTIONS

if TYPE_CHECKING:
    pass


class HarlequinOdbcCursor(HarlequinCursor):
    def __init__(self, cur: pyodbc.Cursor) -> None:
        self.cur = cur
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        # todo: use getTypeInfo
        type_mapping = {
            "bool": "t/f",
            "int": "##",
            "float": "#.#",
            "Decimal": "#.#",
            "str": "s",
            "bytes": "0b",
            "date": "d",
            "time": "t",
            "datetime": "dt",
            "UUID": "uid",
        }
        return [
            (
                col_name if col_name else "(No column name)",
                type_mapping.get(col_type.__name__, "?"),
            )
            for col_name, col_type, *_ in self.cur.description
        ]

    def set_limit(self, limit: int) -> HarlequinOdbcCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e


class HarlequinOdbcConnection(HarlequinConnection):
    def __init__(
        self,
        conn_str: Sequence[str],
        init_message: str = "",
    ) -> None:
        assert len(conn_str) == 1
        self.init_message = init_message
        try:
            self.conn = pyodbc.connect(conn_str[0], autocommit=True)
            self.aux_conn = pyodbc.connect(conn_str[0], autocommit=True)
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to your database."
            ) from e

    def execute(self, query: str) -> HarlequinOdbcCursor | None:
        try:
            cur = self.conn.cursor()
            cur.execute(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=f"{e.__class__.__name__}: {e}",
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur.description is not None:
                return HarlequinOdbcCursor(cur)
            else:
                return None

    def get_catalog(self) -> Catalog:
        raw_catalog = self._list_tables()
        db_items: list[CatalogItem] = []
        for db, schemas in raw_catalog.items():
            schema_items: list[CatalogItem] = []
            for schema, relations in schemas.items():
                rel_items: list[CatalogItem] = []
                for rel, rel_type in relations:
                    rel_items.append(
                        RelationCatalogItem.from_label(
                            label=rel,
                            schema_label=schema,
                            db_label=db,
                            rel_type=rel_type,
                            connection=self,
                        )
                    )
                schema_items.append(
                    SchemaCatalogItem.from_label(
                        label=schema,
                        db_label=db,
                        connection=self,
                        children=rel_items,
                    )
                )
            db_items.append(
                DatabaseCatalogItem.from_label(
                    label=db,
                    connection=self,
                    children=schema_items,
                )
            )
        return Catalog(items=db_items)

    def close(self) -> None:
        with suppress(Exception):
            self.conn.close()
        with suppress(Exception):
            self.aux_conn.close()

    def _list_tables(self) -> dict[str, dict[str, list[tuple[str, str]]]]:
        cur = self.aux_conn.cursor()
        catalog: dict[str, dict[str, list[tuple[str, str]]]] = {}
        for db_name, schema_name, rel_name, rel_type, *_ in cur.tables(catalog="%"):
            if db_name is None:
                continue
            if db_name not in catalog:
                catalog[db_name] = dict()

            if schema_name is None:
                continue
            if schema_name not in catalog[db_name]:
                catalog[db_name][schema_name] = list()

            if rel_name is not None:
                catalog[db_name][schema_name].append((rel_name, rel_type or ""))

        return catalog

    def _list_columns_in_relation(
        self, catalog_name: str, schema_name: str, rel_name: str
    ) -> list[tuple[str, str]]:
        cur = self.aux_conn.cursor()
        raw_cols = cur.columns(table=rel_name, catalog=catalog_name, schema=schema_name)
        return [(col[3], col[5]) for col in raw_cols]

    def get_completions(self) -> list[HarlequinCompletion]:
        return []


class HarlequinOdbcAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = ODBC_OPTIONS

    def __init__(self, conn_str: Sequence[str], **_: Any) -> None:
        self.conn_str = conn_str
        if len(conn_str) != 1:
            raise HarlequinConfigError(
                title="Harlequin could not initialize the ODBC adapter.",
                msg=(
                    "The ODBC adapter expects exactly one connection string. "
                    f"It received:\n{conn_str}"
                ),
            )

    def connect(self) -> HarlequinOdbcConnection:
        conn = HarlequinOdbcConnection(self.conn_str)
        return conn
