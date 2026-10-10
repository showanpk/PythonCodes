from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv("SAHELI_SQL_CONNECTION_STRING")

if not CONNECTION_STRING:
    raise RuntimeError(
        "SAHELI_SQL_CONNECTION_STRING is missing from the .env file."
    )


def build_connection_string() -> str:
    return CONNECTION_STRING
