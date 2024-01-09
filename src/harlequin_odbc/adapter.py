from __future__ import annotations

from typing import Any, Sequence

import pyodbc  # type: ignore
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

from harlequin_odbc.cli_options import ODBC_OPTIONS


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
                msg=str(e),
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
                    cols = self._list_columns_in_relation(
                        catalog_name=db, schema_name=schema, rel_name=rel
                    )
                    col_items = [
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"."{col}"',
                            query_name=f'"{col}"',
                            label=col,
                            type_label=col_type,
                        )
                        for col, col_type in cols
                    ]
                    rel_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"',
                            query_name=f'"{db}"."{schema}"."{rel}"',
                            label=rel,
                            type_label=rel_type,
                            children=col_items,
                        )
                    )
                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{schema}"',
                        query_name=f'"{db}"."{schema}"',
                        label=schema,
                        type_label="s",
                        children=rel_items,
                    )
                )
            db_items.append(
                CatalogItem(
                    qualified_identifier=f'"{db}"',
                    query_name=f'"{db}"',
                    label=db,
                    type_label="db",
                    children=schema_items,
                )
            )
        return Catalog(items=db_items)

    def _list_tables(self) -> dict[str, dict[str, list[tuple[str, str]]]]:
        cur = self.aux_conn.cursor()
        rel_type_map = {
            "TABLE": "t",
            "VIEW": "v",
            "SYSTEM TABLE": "st",
            "GLOBAL TEMPORARY": "tmp",
            "LOCAL TEMPORARY": "tmp",
        }
        catalog: dict[str, dict[str, list[tuple[str, str]]]] = {}
        for db_name, schema_name, rel_name, rel_type, *_ in cur.tables():
            if db_name not in catalog:
                catalog[db_name] = {
                    schema_name: [
                        (rel_name, rel_type_map.get(rel_type, str(rel_type).lower()))
                    ]
                }
            elif schema_name not in catalog[db_name]:
                catalog[db_name][schema_name] = [
                    (rel_name, rel_type_map.get(rel_type, rel_type))
                ]
            else:
                catalog[db_name][schema_name].append(
                    (rel_name, rel_type_map.get(rel_type, rel_type))
                )
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
