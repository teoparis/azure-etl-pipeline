"""
ADLS Gen2 loader: uploads DataFrames as partitioned Parquet files.

Partition layout:
  <container>/<base_path>/<entity>/year=<Y>/month=<M>/day=<D>/<filename>.parquet

Usage:
    factory = AzureClientFactory.from_env()
    loader = ADLSLoader(
        client_factory=factory,
        container="raw",
        base_path="orders",
    )
    loader.load(df, entity="orders", date="2024-01-15")
"""

import io
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from azure.core.exceptions import AzureError

from src.utils.azure_client import AzureClientFactory
from src.utils.logger import get_logger

log = get_logger(__name__)


class ADLSLoaderError(Exception):
    """Raised on ADLS upload failures."""


class ADLSLoader:
    """
    Writes pandas DataFrames to ADLS Gen2 as Snappy-compressed Parquet.

    Features:
      - Date-based partitioning (year/month/day derived from partition columns)
      - Overwrite or append modes
      - Upload verification via file properties check
      - Batch chunking to keep individual Parquet files under a size threshold
    """

    def __init__(
        self,
        client_factory: AzureClientFactory,
        container: str,
        base_path: str = "raw",
        compression: str = "snappy",
        write_mode: str = "overwrite",
        batch_size: int = 100_000,
    ) -> None:
        self._factory = client_factory
        self.container = container
        self.base_path = base_path
        self.compression = compression
        self.write_mode = write_mode
        self.batch_size = batch_size

    # ── Public API ────────────────────────────────────────────────────────────

    def load(
        self,
        df: pd.DataFrame,
        entity: str,
        date: str | None = None,
        filename_prefix: str = "part",
    ) -> list[str]:
        """
        Upload a DataFrame to ADLS Gen2 as one or more Parquet files.

        The output path is:
          <base_path>/<entity>/year=<Y>/month=<M>/day=<D>/<prefix>-<n>.parquet

        Args:
            df:              DataFrame to upload.
            entity:          Logical entity name (e.g. "orders", "customers").
            date:            Partition date as "YYYY-MM-DD". Defaults to today (UTC).
            filename_prefix: Prefix for the generated Parquet file names.

        Returns:
            List of ADLS paths (relative to container root) where files were written.

        Raises:
            ADLSLoaderError: On upload failure or verification error.
        """
        if df.empty:
            log.warning("load() called with empty DataFrame — nothing to write")
            return []

        partition_date = self._parse_date(date)
        partition_path = self._build_partition_path(entity, partition_date)
        datalake = self._factory.get_datalake_service_client()

        log.info(
            "Starting ADLS load",
            entity=entity,
            rows=len(df),
            partition_path=partition_path,
            write_mode=self.write_mode,
        )

        # Ensure the directory exists in ADLS Gen2
        self._ensure_directory(datalake, partition_path)

        if self.write_mode == "overwrite":
            self._delete_existing(datalake, partition_path)

        uploaded_paths: list[str] = []

        # Split into batches to avoid very large Parquet files
        for batch_num, batch_df in enumerate(self._iter_batches(df)):
            filename = f"{filename_prefix}-{batch_num:04d}.parquet"
            remote_path = f"{partition_path}/{filename}"

            parquet_bytes = self._df_to_parquet_bytes(batch_df)
            self._upload_bytes(datalake, remote_path, parquet_bytes)
            self._verify_upload(datalake, remote_path, expected_size=len(parquet_bytes))

            uploaded_paths.append(remote_path)
            log.info(
                "File uploaded",
                path=remote_path,
                size_kb=round(len(parquet_bytes) / 1024, 1),
                rows=len(batch_df),
            )

        log.info(
            "ADLS load complete",
            entity=entity,
            files_written=len(uploaded_paths),
            total_rows=len(df),
        )
        return uploaded_paths

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _build_partition_path(self, entity: str, date: datetime) -> str:
        """Build the ADLS directory path for the given entity and date."""
        return (
            f"{self.base_path}/{entity}"
            f"/year={date.year}"
            f"/month={date.strftime('%m')}"
            f"/day={date.strftime('%d')}"
        )

    def _parse_date(self, date_str: str | None) -> datetime:
        if date_str is None:
            return datetime.utcnow()
        return datetime.strptime(date_str, "%Y-%m-%d")

    def _iter_batches(self, df: pd.DataFrame):
        """Yield DataFrame slices of self.batch_size rows."""
        for start in range(0, len(df), self.batch_size):
            yield df.iloc[start : start + self.batch_size]

    def _df_to_parquet_bytes(self, df: pd.DataFrame) -> bytes:
        """Serialise a DataFrame to Parquet bytes in memory."""
        table = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression=self.compression,
            use_dictionary=True,
            write_statistics=True,
        )
        return buf.getvalue()

    def _ensure_directory(self, datalake, path: str) -> None:
        """Create the directory path in ADLS Gen2 (no-op if already exists)."""
        try:
            fs_client = datalake.get_file_system_client(self.container)
            fs_client.create_directory(path)
        except AzureError as e:
            # Directory may already exist — log and continue
            log.debug("Directory create (may already exist)", path=path, error=str(e)[:80])

    def _delete_existing(self, datalake, path: str) -> None:
        """Delete existing files in the partition directory (overwrite mode)."""
        try:
            fs_client = datalake.get_file_system_client(self.container)
            dir_client = fs_client.get_directory_client(path)
            # List and delete existing Parquet files only (don't nuke the dir)
            for item in dir_client.get_paths():
                if str(item.name).endswith(".parquet"):
                    fs_client.get_file_client(item.name).delete_file()
                    log.debug("Deleted existing file", path=item.name)
        except AzureError as e:
            log.debug("Overwrite cleanup skipped (path may not exist)", error=str(e)[:80])

    def _upload_bytes(self, datalake, remote_path: str, data: bytes) -> None:
        """Upload raw bytes to a file path in ADLS Gen2."""
        try:
            fs_client = datalake.get_file_system_client(self.container)
            file_client = fs_client.get_file_client(remote_path)
            file_client.upload_data(data, overwrite=True)
        except AzureError as e:
            raise ADLSLoaderError(f"Upload failed for {remote_path}: {e}") from e

    def _verify_upload(
        self, datalake, remote_path: str, expected_size: int
    ) -> None:
        """Verify the uploaded file exists and matches expected byte size."""
        try:
            fs_client = datalake.get_file_system_client(self.container)
            props = fs_client.get_file_client(remote_path).get_file_properties()
            actual_size = props.size
            if actual_size != expected_size:
                raise ADLSLoaderError(
                    f"Size mismatch on {remote_path}: "
                    f"expected {expected_size}, got {actual_size}"
                )
        except AzureError as e:
            raise ADLSLoaderError(
                f"Verification failed for {remote_path}: {e}"
            ) from e
