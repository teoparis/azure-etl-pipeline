"""
JDBC / pyodbc extractor with chunked reads to avoid OOM on large tables.

Usage:
    extractor = JDBCExtractor.from_env(config["sources"]["jdbc"])
    df = extractor.extract_query(
        query="SELECT * FROM orders WHERE updated_at >= ?",
        params=("2024-01-15",),
    )
"""

import os
from typing import Iterator

import pandas as pd
import pyodbc

from src.utils.logger import get_logger

log = get_logger(__name__)


class JDBCExtractorError(Exception):
    """Raised on connection or query errors."""


class JDBCExtractor:
    """
    SQL Server / Azure SQL extractor using pyodbc.

    Reads large result sets in chunks (chunksize rows per iteration)
    and concatenates them into a single DataFrame, preventing OOM on
    tables with millions of rows.
    """

    def __init__(
        self,
        driver: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        extra_params: str = "Encrypt=yes;TrustServerCertificate=no",
        chunk_size: int = 10_000,
    ) -> None:
        self.driver = driver
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.extra_params = extra_params
        self.chunk_size = chunk_size
        self._conn: pyodbc.Connection | None = None

    @classmethod
    def from_env(cls, source_config: dict) -> "JDBCExtractor":
        """Build extractor from config dict + env vars."""
        password_env = source_config.get("password_env", "JDBC_PASSWORD")
        return cls(
            driver=os.environ.get("JDBC_DRIVER", source_config["driver"]),
            host=os.environ.get("JDBC_HOST", source_config["host"]),
            port=int(os.environ.get("JDBC_PORT", source_config.get("port", 1433))),
            database=os.environ.get("JDBC_DATABASE", source_config["database"]),
            username=os.environ.get("JDBC_USERNAME", source_config["username"]),
            password=os.environ[password_env],
            extra_params=source_config.get("extra_params", "Encrypt=yes"),
            chunk_size=source_config.get("chunk_size", 10_000),
        )

    # ── Connection management ─────────────────────────────────────────────────

    def _connection_string(self) -> str:
        return (
            f"DRIVER={self.driver};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"{self.extra_params}"
        )

    def connect(self) -> None:
        """Open the database connection."""
        log.info(
            "Connecting to SQL Server",
            host=self.host,
            port=self.port,
            database=self.database,
        )
        try:
            self._conn = pyodbc.connect(
                self._connection_string(), timeout=30, autocommit=True
            )
        except pyodbc.Error as e:
            raise JDBCExtractorError(f"Connection failed: {e}") from e

    def disconnect(self) -> None:
        """Close the database connection if open."""
        if self._conn:
            self._conn.close()
            self._conn = None
            log.debug("SQL connection closed")

    def __enter__(self) -> "JDBCExtractor":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.disconnect()

    # ── Query execution ───────────────────────────────────────────────────────

    def extract_query(
        self,
        query: str,
        params: tuple = (),
    ) -> pd.DataFrame:
        """
        Execute a SQL query and return the full result as a DataFrame.

        Reads in chunks of self.chunk_size rows to bound memory usage.
        The final DataFrame is the concatenation of all chunks.

        Args:
            query:  SQL query string. Use ? placeholders for parameters.
            params: Tuple of parameter values corresponding to ? placeholders.

        Returns:
            pd.DataFrame with all result rows.

        Raises:
            JDBCExtractorError: On connection or query errors.
        """
        if self._conn is None:
            self.connect()

        log.info("Executing query", query=query[:120], params=params)

        try:
            chunks: list[pd.DataFrame] = []
            total_rows = 0

            for chunk in self._iter_chunks(query, params):
                chunks.append(chunk)
                total_rows += len(chunk)
                log.debug("Chunk read", chunk_rows=len(chunk), total_so_far=total_rows)

            if not chunks:
                log.warning("Query returned 0 rows", query=query[:120])
                return pd.DataFrame()

            df = pd.concat(chunks, ignore_index=True)
            log.info("Query complete", total_rows=len(df), columns=list(df.columns))
            return df

        except pyodbc.Error as e:
            raise JDBCExtractorError(f"Query failed: {e}") from e

    def _iter_chunks(
        self, query: str, params: tuple
    ) -> Iterator[pd.DataFrame]:
        """Yield DataFrames of self.chunk_size rows."""
        cursor = self._conn.cursor()
        cursor.execute(query, params)

        columns = [col[0] for col in cursor.description]
        while True:
            rows = cursor.fetchmany(self.chunk_size)
            if not rows:
                break
            yield pd.DataFrame(rows, columns=columns)

        cursor.close()

    def extract_table(
        self,
        table: str,
        where_clause: str = "",
        params: tuple = (),
    ) -> pd.DataFrame:
        """
        Convenience method to extract a full table (with optional WHERE clause).

        Args:
            table:        Schema-qualified table name (e.g. "dbo.orders").
            where_clause: Optional WHERE clause without the WHERE keyword.
            params:       Parameters for the WHERE clause.
        """
        query = f"SELECT * FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"
        return self.extract_query(query, params)
