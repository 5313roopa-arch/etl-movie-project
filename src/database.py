import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import DATABASE_PATH


def ensure_database_directory() -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    ensure_database_directory()
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON;')
    return connection


def drop_database() -> None:
    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()


def load_schema() -> str:
    schema_file = Path(__file__).resolve().parent.parent / 'sql' / 'schema.sql'
    return schema_file.read_text()


def create_schema(connection: sqlite3.Connection) -> None:
    schema_script = load_schema()
    connection.executescript(schema_script)


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Cursor]:
    cursor = connection.cursor()
    try:
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
