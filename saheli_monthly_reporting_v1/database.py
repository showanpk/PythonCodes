\
from __future__ import annotations

import pyodbc
from config import build_connection_string


def connect() -> pyodbc.Connection:
    connection_string = build_connection_string()
    connection = pyodbc.connect(connection_string, autocommit=True)
    return connection
