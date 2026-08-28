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
from harlequin.catalog import (
    Catalog,
    CatalogItem,
    CatalogSearchKind,
    CatalogSearchResult,
)
from harlequin.exception import (
    HarlequinConfigError,
    HarlequinConnectionError,
    HarlequinQueryError,
)
from textual_fastdatatable.backend import AutoBackendType

from harlequin_odbc.catalog import (
    ColumnCatalogItem,
    DatabaseCatalogItem,
    RelationCatalogItem,
    SchemaCatalogItem,
)
from harlequin_odbc.cli_options import ODBC_OPTIONS

if TYPE_CHECKING:
    pass


def _contains(label: str, folded_term: str) -> bool:
    """True if label contains the (already case-folded) term."""
    return folded_term in label.casefold()


def _column_results(
    rel_item: RelationCatalogItem, cols: list[tuple[str, str]]
) -> list[CatalogSearchResult]:
    """The search results for columns matched in one relation."""
    return [
        CatalogSearchResult(
            item=ColumnCatalogItem.from_parent(
                parent=rel_item, label=label, type_label=type_label
            ),
            parents=(rel_item.db_label, rel_item.schema_label, rel_item.label),
        )
        for label, type_label in cols
    ]


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
        self._pattern_escape: str | None = None
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

    def search_catalog(
        self, term: str, kind: CatalogSearchKind = "all"
    ) -> list[CatalogSearchResult]:
        folded_term = term.casefold()
        try:
            # one call lists every relation on the server, which is what
            # get_catalog() already asks for, and is where the databases and
            # schemas come from -- so every level above a column is matched
            # here, case-insensitively, no matter how a driver reads a pattern
            raw_catalog = self._list_tables()
            matched_columns = (
                self._search_columns(term, list(raw_catalog))
                if kind in ("columns", "all")
                else {}
            )
        except Exception as e:
            raise HarlequinQueryError(
                msg=f"{e.__class__.__name__}: {e}",
                title="Harlequin encountered an error while searching your catalog.",
            ) from e

        results: list[CatalogSearchResult] = []
        for db, schemas in raw_catalog.items():
            if kind == "all" and _contains(db, folded_term):
                results.append(
                    CatalogSearchResult(
                        item=DatabaseCatalogItem.from_label(label=db, connection=self)
                    )
                )
            for schema, relations in schemas.items():
                if kind == "all" and _contains(schema, folded_term):
                    results.append(
                        CatalogSearchResult(
                            item=SchemaCatalogItem.from_label(
                                label=schema, db_label=db, connection=self
                            ),
                            parents=(db,),
                        )
                    )
                for rel, rel_type in relations:
                    cols = matched_columns.pop((db, schema, rel), [])
                    matched_rel = kind in ("relations", "all") and _contains(
                        rel, folded_term
                    )
                    if not cols and not matched_rel:
                        continue
                    rel_item = RelationCatalogItem.from_label(
                        label=rel,
                        schema_label=schema,
                        db_label=db,
                        rel_type=rel_type,
                        connection=self,
                    )
                    if matched_rel:
                        results.append(
                            CatalogSearchResult(item=rel_item, parents=(db, schema))
                        )
                    results.extend(_column_results(rel_item, cols))

        # a driver can name a relation in its column listing that it left out
        # of its table listing; the column still matched, so it is still a hit
        for (db, schema, rel), cols in matched_columns.items():
            results.extend(
                _column_results(
                    RelationCatalogItem.from_label(
                        label=rel,
                        schema_label=schema,
                        db_label=db,
                        rel_type="",
                        connection=self,
                    ),
                    cols,
                )
            )
        return results

    def _search_columns(
        self, term: str, db_names: list[str]
    ) -> dict[tuple[str, str, str], list[tuple[str, str]]]:
        """Columns whose names contain term, keyed by the relation that has them.

        SQLColumns takes its catalog as a name and not as a pattern, so this is
        one call per database -- still one call for the level, and not one per
        relation, which is the walk that a search exists to avoid.
        """
        folded_term = term.casefold()
        pattern = self._contains_pattern(term)
        cur = self.aux_conn.cursor()
        matches: dict[tuple[str, str, str], list[tuple[str, str]]] = {}
        for db in db_names:
            raw_cols = cur.columns(catalog=db, schema="%", table="%", column=pattern)
            for (
                db_name,
                schema_name,
                rel_name,
                col_name,
                _data_type,
                type_name,
                *_,
            ) in raw_cols:
                # how a driver reads the pattern is the driver's: it may be
                # case-sensitive, or may not take the escape. Matching again
                # here drops whatever extra it let through, so a hit always
                # means what the contract says it does.
                if col_name is None or not _contains(col_name, folded_term):
                    continue
                if schema_name is None or rel_name is None:
                    continue
                matches.setdefault((db_name or db, schema_name, rel_name), []).append(
                    (col_name, type_name or "")
                )
        return matches

    def _contains_pattern(self, term: str) -> str:
        """The term as an ODBC pattern matching any name that contains it."""
        escape = self._get_pattern_escape()
        if not escape:
            # with no escape character to use, % and _ in the term stay
            # metacharacters, and the pattern matches more than was asked for;
            # _search_columns() drops the extra when it matches again
            return f"%{term}%"
        escaped = term
        for character in (escape, "%", "_"):
            escaped = escaped.replace(character, f"{escape}{character}")
        return f"%{escaped}%"

    def _get_pattern_escape(self) -> str:
        """The character this driver escapes pattern metacharacters with."""
        if self._pattern_escape is None:
            try:
                escape = self.aux_conn.getinfo(pyodbc.SQL_SEARCH_PATTERN_ESCAPE)
            except Exception:
                escape = ""
            self._pattern_escape = escape if isinstance(escape, str) else ""
        return self._pattern_escape

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
    IMPLEMENTS_CATALOG_SEARCH = True

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
