import pyodbc
from config import build_connection_string


def connect():
    return pyodbc.connect(
        build_connection_string(),
        autocommit=True
    )