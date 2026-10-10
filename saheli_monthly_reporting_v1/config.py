\
from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def build_connection_string() -> str:
    direct = os.getenv("SQL_CONNECTION_STRING", "").strip()
    if direct:
        return direct

    server = os.getenv("SQL_SERVER", "").strip()
    database = os.getenv("SQL_DATABASE", "").strip()
    auth = os.getenv("SQL_AUTH", "interactive").strip().lower()
    driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server").strip()

    if not server or not database:
        raise RuntimeError(
            "Set SQL_CONNECTION_STRING or SQL_SERVER + SQL_DATABASE in .env."
        )

    common = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    if auth == "interactive":
        return common + "Authentication=ActiveDirectoryInteractive;"

    if auth == "password":
        username = os.getenv("SQL_USERNAME", "").strip()
        password = os.getenv("SQL_PASSWORD", "")
        if not username or not password:
            raise RuntimeError(
                "SQL_AUTH=password requires SQL_USERNAME and SQL_PASSWORD."
            )
        return common + f"UID={username};PWD={password};"

    raise RuntimeError("SQL_AUTH must be 'interactive' or 'password'.")
