"""Shared AAD-authenticated ODBC connection helper for Fabric SQL analytics
endpoints (Lakehouse) and Warehouses. Same pattern as sibling POCs — no
passwords, no connection strings with embedded credentials.
"""
from __future__ import annotations

import struct

import pyodbc
from azure.identity import DefaultAzureCredential

SQL_COPT_SS_ACCESS_TOKEN = 1256
AAD_SQL_SCOPE = "https://database.windows.net/.default"


def get_sql_connection(server: str, database: str) -> pyodbc.Connection:
    token = DefaultAzureCredential().get_token(AAD_SQL_SCOPE)
    token_bytes = token.token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};DATABASE={database};Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}, timeout=60)
