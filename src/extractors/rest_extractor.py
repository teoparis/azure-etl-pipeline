"""
REST API extractor with pagination, bearer auth, and tenacity retry.

Supports two pagination strategies:
  - next_page_token: follow a token embedded in the response body
  - offset: classic limit/offset pagination

Usage:
    extractor = RESTExtractor.from_env(config["sources"]["rest_api"])
    records = extractor.extract(endpoint="/v2/orders", params={"updated_after": "2024-01-15"})
"""

import os
import time
from typing import Any, Generator

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.utils.logger import get_logger

log = get_logger(__name__)

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class RESTExtractorError(Exception):
    """Raised when the extractor encounters a non-retryable error."""


class RESTExtractor:
    """
    Generic REST API extractor.

    Handles:
      - Bearer token authentication
      - next_page_token and offset pagination
      - Exponential backoff retry via tenacity
      - Rate limit detection (429 → honour Retry-After header)
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        page_size: int = 500,
        pagination_strategy: str = "next_page_token",
        token_field: str = "next_page_token",
        results_field: str = "data",
        timeout: int = 30,
        max_pages: int = 1000,
        retry_attempts: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.pagination_strategy = pagination_strategy
        self.token_field = token_field
        self.results_field = results_field
        self.timeout = timeout
        self.max_pages = max_pages
        self.retry_attempts = retry_attempts

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    @classmethod
    def from_env(cls, source_config: dict) -> "RESTExtractor":
        """Build extractor from config dict + env vars."""
        token_env = source_config["auth"]["token_env"]
        token = os.environ[token_env]
        pagination = source_config.get("pagination", {})

        return cls(
            base_url=os.environ.get("REST_API_BASE_URL", source_config["base_url"]),
            token=token,
            page_size=pagination.get("page_size", 500),
            pagination_strategy=pagination.get("strategy", "next_page_token"),
            token_field=pagination.get("token_field", "next_page_token"),
            results_field=pagination.get("results_field", "data"),
            timeout=source_config.get("timeout_seconds", 30),
            max_pages=source_config.get("max_pages", 1000),
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def extract(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> list[dict]:
        """
        Fetch all pages from the given endpoint and return a flat list of records.

        Args:
            endpoint: Path relative to base_url (e.g. "/v2/orders").
            params:   Additional query parameters (e.g. filters, date ranges).

        Returns:
            Flat list of record dicts.

        Raises:
            RESTExtractorError: On non-retryable HTTP errors or if max_pages exceeded.
        """
        url = f"{self.base_url}{endpoint}"
        all_records: list[dict] = []
        params = dict(params or {})

        log.info(
            "Starting REST extraction",
            url=url,
            pagination=self.pagination_strategy,
            params=params,
        )

        if self.pagination_strategy == "next_page_token":
            all_records = list(self._paginate_token(url, params))
        elif self.pagination_strategy == "offset":
            all_records = list(self._paginate_offset(url, params))
        else:
            raise RESTExtractorError(
                f"Unknown pagination strategy: {self.pagination_strategy}"
            )

        log.info(
            "REST extraction complete",
            url=url,
            total_records=len(all_records),
        )
        return all_records

    # ── Pagination strategies ─────────────────────────────────────────────────

    def _paginate_token(
        self, url: str, params: dict
    ) -> Generator[dict, None, None]:
        """Yield records following next_page_token pagination."""
        page_token: str | None = None
        page = 0

        while True:
            if page >= self.max_pages:
                raise RESTExtractorError(
                    f"Exceeded max_pages={self.max_pages} — possible infinite pagination"
                )

            req_params = {**params, "limit": self.page_size}
            if page_token:
                req_params[self.token_field] = page_token

            response = self._get_with_retry(url, req_params)
            records = response.get(self.results_field, [])

            log.debug(
                "Page fetched",
                page=page,
                records_in_page=len(records),
                next_token=bool(response.get(self.token_field)),
            )

            yield from records
            page += 1

            page_token = response.get(self.token_field)
            if not page_token:
                break

    def _paginate_offset(
        self, url: str, params: dict
    ) -> Generator[dict, None, None]:
        """Yield records using limit/offset pagination."""
        offset = 0
        page = 0

        while True:
            if page >= self.max_pages:
                raise RESTExtractorError(
                    f"Exceeded max_pages={self.max_pages}"
                )

            req_params = {**params, "limit": self.page_size, "offset": offset}
            response = self._get_with_retry(url, req_params)
            records = response.get(self.results_field, [])

            log.debug("Page fetched", page=page, offset=offset, records=len(records))

            if not records:
                break

            yield from records
            page += 1
            offset += len(records)

            # If we got fewer records than page_size, we're on the last page
            if len(records) < self.page_size:
                break

    # ── HTTP layer ────────────────────────────────────────────────────────────

    def _get_with_retry(self, url: str, params: dict) -> dict:
        """
        Perform a GET request with tenacity-based exponential backoff retry.
        Handles 429 rate-limit by honouring the Retry-After header.
        """

        @retry(
            stop=stop_after_attempt(self.retry_attempts),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
            reraise=True,
        )
        def _get() -> dict:
            resp = self._session.get(url, params=params, timeout=self.timeout)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 10))
                log.warning("Rate limited, sleeping", retry_after=retry_after)
                time.sleep(retry_after)
                resp = self._session.get(url, params=params, timeout=self.timeout)

            if resp.status_code in _RETRYABLE_STATUS:
                log.warning(
                    "Retryable HTTP error",
                    status=resp.status_code,
                    url=url,
                )
                resp.raise_for_status()

            if not resp.ok:
                raise RESTExtractorError(
                    f"Non-retryable HTTP {resp.status_code} on {url}: {resp.text[:200]}"
                )

            return resp.json()

        return _get()
