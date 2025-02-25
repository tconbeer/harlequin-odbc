from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from harlequin.catalog import CatalogItem, InteractiveCatalogItem

from harlequin_odbc.interactions import (
    execute_drop_database_statement,
    execute_drop_table_statement,
    execute_drop_view_statement,
    execute_use_statement,
    insert_columns_at_cursor,
    show_select_star,
)

if TYPE_CHECKING:
    from harlequin_odbc.adapter import HarlequinOdbcConnection


@dataclass
class ColumnCatalogItem(InteractiveCatalogItem["HarlequinOdbcConnection"]):
    parent: "RelationCatalogItem" | None = None

    @classmethod
    def from_parent(
        cls,
        parent: "RelationCatalogItem",
        label: str,
        type_label: str,
    ) -> "ColumnCatalogItem":
        column_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        column_query_name = f'"{label}"'
        return cls(
            qualified_identifier=column_qualified_identifier,
            query_name=column_query_name,
            label=label,
            type_label=type_label,
            connection=parent.connection,
            parent=parent,
            loaded=True,
        )


@dataclass
class RelationCatalogItem(InteractiveCatalogItem["HarlequinOdbcConnection"]):
    INTERACTIONS = [
        ("Insert Columns at Cursor", insert_columns_at_cursor),
        ("Preview Data", show_select_star),
    ]
    TYPE_LABEL: ClassVar[str] = ""
    schema_label: str = ""
    db_label: str = ""

    @classmethod
    def from_label(
        cls,
        label: str,
        schema_label: str,
        db_label: str,
        rel_type: str,
        connection: "HarlequinOdbcConnection",
    ) -> "RelationCatalogItem":
        rel_type_map: dict[str, type[RelationCatalogItem]] = {
            "TABLE": TableCatalogItem,
            "VIEW": ViewCatalogItem,
            "SYSTEM TABLE": SystemTableCatalogItem,
            "GLOBAL TEMPORARY": GlobalTempTableCatalogItem,
            "LOCAL TEMPORARY": LocalTempTableCatalogItem,
        }

        item_class = rel_type_map.get(rel_type, TableCatalogItem)
        return item_class(
            qualified_identifier=f'"{db_label}"."{schema_label}"."{label}"',
            query_name=f'"{schema_label}"."{label}"',
            label=label,
            schema_label=schema_label,
            db_label=db_label,
            type_label=item_class.TYPE_LABEL,
            connection=connection,
        )

    def fetch_children(self) -> list[ColumnCatalogItem]:
        if self.connection is None:
            return []
        cols = self.connection._list_columns_in_relation(
            catalog_name=self.db_label,
            schema_name=self.schema_label,
            rel_name=self.label,
        )
        return [
            ColumnCatalogItem.from_parent(
                parent=self, label=col_label, type_label=col_type_label
            )
            for col_label, col_type_label in cols
        ]


class ViewCatalogItem(RelationCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Drop View", execute_drop_view_statement),
    ]
    TYPE_LABEL: ClassVar[str] = "v"


class TableCatalogItem(RelationCatalogItem):
    INTERACTIONS = RelationCatalogItem.INTERACTIONS + [
        ("Drop Table", execute_drop_table_statement),
    ]
    TYPE_LABEL: ClassVar[str] = "t"


class SystemTableCatalogItem(RelationCatalogItem):
    TYPE_LABEL: ClassVar[str] = "st"


class TempTableCatalogItem(TableCatalogItem):
    TYPE_LABEL: ClassVar[str] = "tmp"


class GlobalTempTableCatalogItem(TempTableCatalogItem):
    pass


class LocalTempTableCatalogItem(TempTableCatalogItem):
    pass


@dataclass
class SchemaCatalogItem(InteractiveCatalogItem["HarlequinOdbcConnection"]):
    db_label: str = ""

    @classmethod
    def from_label(
        cls,
        label: str,
        db_label: str,
        connection: "HarlequinOdbcConnection",
        children: list[CatalogItem] | None = None,
    ) -> "SchemaCatalogItem":
        schema_identifier = f'"{label}"'
        if children is None:
            children = []
        return cls(
            qualified_identifier=f'"{db_label}".{schema_identifier}',
            query_name=schema_identifier,
            label=label,
            db_label=db_label,
            type_label="sch",
            connection=connection,
            children=children,
            loaded=True,
        )


class DatabaseCatalogItem(InteractiveCatalogItem["HarlequinOdbcConnection"]):
    INTERACTIONS = [
        ("Use Database", execute_use_statement),
        ("Drop Database", execute_drop_database_statement),
    ]

    @classmethod
    def from_label(
        cls,
        label: str,
        connection: "HarlequinOdbcConnection",
        children: list[CatalogItem] | None = None,
    ) -> "DatabaseCatalogItem":
        database_identifier = f'"{label}"'
        if children is None:
            children = []
        return cls(
            qualified_identifier=database_identifier,
            query_name=database_identifier,
            label=label,
            type_label="db",
            connection=connection,
            children=children,
            loaded=True,
        )
