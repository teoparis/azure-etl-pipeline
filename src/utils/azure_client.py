"""
Azure SDK client factory.

Supports two authentication strategies:
  1. DefaultAzureCredential — for production (service principal via env vars,
     managed identity, etc.)
  2. Connection string — for local development / testing

Usage:
    factory = AzureClientFactory.from_env()
    datalake_client = factory.get_datalake_service_client()
    blob_client = factory.get_blob_service_client()
"""

import os
from functools import lru_cache

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.pipeline.policies import RetryPolicy

from src.utils.logger import get_logger

log = get_logger(__name__)


class AzureClientFactory:
    """
    Factory that creates and caches Azure SDK clients.

    All clients share the same credential and retry policy so we don't
    instantiate multiple auth flows for a single pipeline run.
    """

    def __init__(
        self,
        storage_account: str,
        credential,
        max_retries: int = 3,
        retry_backoff: float = 0.8,
    ) -> None:
        self._account = storage_account
        self._credential = credential
        self._retry_policy = RetryPolicy(
            retry_total=max_retries,
            retry_backoff_factor=retry_backoff,
        )
        self._datalake_client: DataLakeServiceClient | None = None
        self._blob_client: BlobServiceClient | None = None

    # ── Constructors ──────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "AzureClientFactory":
        """
        Build a factory using environment variables.

        Prefers connection string if AZURE_STORAGE_CONNECTION_STRING is set
        (handy for Azurite local emulator or dev accounts).
        Falls back to DefaultAzureCredential (works with SP env vars, managed
        identity, Azure CLI login, etc.).
        """
        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        storage_account = os.environ["AZURE_STORAGE_ACCOUNT"]

        if conn_str:
            log.info(
                "Azure auth: connection string",
                storage_account=storage_account,
            )
            return cls._from_connection_string(conn_str, storage_account)

        tenant_id = os.getenv("AZURE_TENANT_ID")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")

        if tenant_id and client_id and client_secret:
            log.info(
                "Azure auth: service principal",
                tenant_id=tenant_id,
                client_id=client_id,
                storage_account=storage_account,
            )
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
        else:
            log.info(
                "Azure auth: DefaultAzureCredential",
                storage_account=storage_account,
            )
            credential = DefaultAzureCredential()

        return cls(storage_account=storage_account, credential=credential)

    @classmethod
    def _from_connection_string(
        cls, conn_str: str, storage_account: str
    ) -> "AzureClientFactory":
        """Internal: create a factory that uses a connection string directly."""
        instance = cls.__new__(cls)
        instance._account = storage_account
        instance._credential = None  # not used with conn_str path
        instance._retry_policy = RetryPolicy(retry_total=3)
        instance._conn_str = conn_str
        instance._datalake_client = None
        instance._blob_client = None
        return instance

    # ── Client getters (lazy, cached per instance) ────────────────────────────

    def get_datalake_service_client(self) -> DataLakeServiceClient:
        """Return (or create) a DataLakeServiceClient for ADLS Gen2."""
        if self._datalake_client is None:
            conn_str = getattr(self, "_conn_str", None)
            if conn_str:
                self._datalake_client = DataLakeServiceClient.from_connection_string(
                    conn_str
                )
            else:
                account_url = f"https://{self._account}.dfs.core.windows.net"
                self._datalake_client = DataLakeServiceClient(
                    account_url=account_url,
                    credential=self._credential,
                    retry_policy=self._retry_policy,
                )
            log.debug(
                "DataLakeServiceClient created",
                account=self._account,
            )
        return self._datalake_client

    def get_blob_service_client(self) -> BlobServiceClient:
        """Return (or create) a BlobServiceClient."""
        if self._blob_client is None:
            conn_str = getattr(self, "_conn_str", None)
            if conn_str:
                self._blob_client = BlobServiceClient.from_connection_string(conn_str)
            else:
                account_url = f"https://{self._account}.blob.core.windows.net"
                self._blob_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self._credential,
                    retry_policy=self._retry_policy,
                )
            log.debug("BlobServiceClient created", account=self._account)
        return self._blob_client
