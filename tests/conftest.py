from __future__ import annotations

from typing import Generator

import pyodbc
import pytest

from harlequin_odbc.adapter import (
    HarlequinOdbcAdapter,
    HarlequinOdbcConnection,
)

MASTER_DB_CONN = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:localhost,1433;Database=master;Uid=sa;Pwd={for-testing};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=5;"  # noqa: E501
TEST_DB_CONN = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:localhost,1433;Database=test;Uid=sa;Pwd={for-testing};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=5;"  # noqa: E501


@pytest.fixture
def connection() -> Generator[HarlequinOdbcConnection, None, None]:
    master_conn = pyodbc.connect(MASTER_DB_CONN, autocommit=True)
    cur = master_conn.cursor()
    cur.execute("drop database if exists test;")
    cur.execute("create database test;")
    cur.close()
    master_conn.close()
    conn = HarlequinOdbcAdapter(conn_str=(TEST_DB_CONN,)).connect()
    yield conn
    conn.close()
