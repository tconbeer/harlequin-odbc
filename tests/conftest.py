from __future__ import annotations

import uuid
from typing import Generator

import pyodbc
import pytest

from harlequin_odbc.adapter import (
    HarlequinOdbcAdapter,
    HarlequinOdbcConnection,
)

MASTER_DB_CONN = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:localhost,1433;Database=master;Uid=sa;Pwd={For-Testing1};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=5;"  # noqa: E501
TEST_DB_CONN = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:localhost,1433;Database=test;Uid=sa;Pwd={For-Testing1};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=5;"  # noqa: E501


@pytest.fixture
def connection() -> Generator[HarlequinOdbcConnection, None, None]:
    master_conn = pyodbc.connect(MASTER_DB_CONN, autocommit=True)
    cur = master_conn.cursor()
    # a connection from a previous test can still be pooled, and SQL Server
    # will not drop a database that anything is connected to
    cur.execute(
        "if db_id('test') is not null "
        "alter database test set single_user with rollback immediate;"
    )
    cur.execute("drop database if exists test;")
    cur.execute("create database test;")
    cur.close()
    master_conn.close()
    conn = HarlequinOdbcAdapter(conn_str=(TEST_DB_CONN,)).connect()
    yield conn
    conn.close()


@pytest.fixture
def connection_without_quoted_identifiers(
    connection: HarlequinOdbcConnection,
) -> Generator[HarlequinOdbcConnection, None, None]:
    """A second connection to the test database, with QUOTED_IDENTIFIER off."""
    conn = HarlequinOdbcAdapter(conn_str=(f"{TEST_DB_CONN}QuotedId=No;",)).connect()
    yield conn
    conn.close()


@pytest.fixture
def quoted_database_name() -> Generator[str, None, None]:
    """A database whose name needs quoting, dropped afterward."""
    database_name = f"harlequin odbc \"q]'.; {uuid.uuid4().hex[:8]}"
    master_conn = pyodbc.connect(MASTER_DB_CONN, autocommit=True)
    master_cur = master_conn.cursor()
    master_cur.execute(
        "declare @s nvarchar(max) = N'create database ' + quotename(?); exec(@s)",
        database_name,
    )
    yield database_name
    master_cur.execute(
        "if db_id(?) is not null begin "
        "declare @s nvarchar(max) = N'alter database ' + quotename(?) "
        "+ N' set single_user with rollback immediate; drop database ' "
        "+ quotename(?); exec(@s) end",
        database_name,
        database_name,
        database_name,
    )
    master_conn.close()
