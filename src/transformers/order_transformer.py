"""
Order data transformer.

Responsibilities:
  - Normalize field names (snake_case, consistent naming)
  - Cast types (dates, numeric amounts)
  - Deduplicate on order_id (keep latest updated_at)
  - Compute derived columns: order_value_eur, is_international
  - Handle nulls with sensible defaults
  - Add partition columns (year, month, day) from order_date

Input schema (raw REST/JDBC records):
  order_id, customer_id, status, currency, amount, created_at, updated_at,
  shipping_country, billing_country, items (list), discount_amount (optional)

Output schema (normalised):
  order_id, customer_id, status, currency, amount_original, discount_amount,
  net_amount, order_value_eur, is_international, order_date, created_at,
  updated_at, shipping_country, billing_country, item_count,
  year, month, day
"""

from datetime import datetime
from typing import Any

import pandas as pd

from src.utils.logger import get_logger

log = get_logger(__name__)

# EUR exchange rates (static approximation — replace with live FX in prod)
_FX_TO_EUR: dict[str, float] = {
    "EUR": 1.0,
    "USD": 0.92,
    "GBP": 1.17,
    "CHF": 1.03,
    "SEK": 0.087,
    "NOK": 0.086,
    "DKK": 0.134,
    "PLN": 0.23,
    "CZK": 0.041,
    "HUF": 0.0026,
}

_DOMESTIC_COUNTRY = "IT"  # pivot country for is_international logic


class OrderTransformerError(Exception):
    """Raised when transformation fails on unrecoverable input."""


class OrderTransformer:
    """
    Stateless transformer for raw order records.

    All methods are pure functions operating on DataFrames; the class
    mainly serves as a namespace and configuration holder.
    """

    def __init__(
        self,
        domestic_country: str = _DOMESTIC_COUNTRY,
        fx_rates: dict[str, float] | None = None,
    ) -> None:
        self.domestic_country = domestic_country
        self.fx_rates = fx_rates or _FX_TO_EUR

    # ── Public API ────────────────────────────────────────────────────────────

    def transform(self, records: list[dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
        """
        Full transformation pipeline.

        Steps:
          1. Cast to DataFrame
          2. Normalize column names
          3. Cast and coerce types
          4. Handle nulls
          5. Deduplicate
          6. Compute derived columns
          7. Add partition columns

        Args:
            records: Raw records from extractor (list of dicts or DataFrame).

        Returns:
            Clean, enriched DataFrame ready for ADLS load.
        """
        if isinstance(records, list):
            df = pd.DataFrame(records)
        else:
            df = records.copy()

        if df.empty:
            log.warning("OrderTransformer received empty input")
            return df

        log.info("Starting transformation", input_rows=len(df))

        df = self._normalize_columns(df)
        df = self._cast_types(df)
        df = self._fill_nulls(df)
        df = self._deduplicate(df)
        df = self._add_derived_columns(df)
        df = self._add_partition_columns(df)
        df = self._select_output_columns(df)

        log.info("Transformation complete", output_rows=len(df))
        return df

    # ── Transformation steps ──────────────────────────────────────────────────

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lowercase all columns and apply known field aliases."""
        df.columns = [c.lower().strip() for c in df.columns]

        # Alias mapping: handle different field names from different sources
        aliases = {
            "id": "order_id",
            "orderid": "order_id",
            "order_number": "order_id",
            "customerid": "customer_id",
            "totalamount": "amount",
            "total_amount": "amount",
            "grand_total": "amount",
            "createdat": "created_at",
            "updatedat": "updated_at",
            "orderstatus": "status",
            "shipcountry": "shipping_country",
            "ship_country": "shipping_country",
            "billcountry": "billing_country",
            "bill_country": "billing_country",
        }
        df = df.rename(columns=aliases)

        log.debug("Columns normalized", columns=list(df.columns))
        return df

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns to expected types."""
        if "order_id" in df.columns:
            df["order_id"] = df["order_id"].astype(str).str.strip()

        if "customer_id" in df.columns:
            df["customer_id"] = df["customer_id"].astype(str).str.strip()

        for col in ("amount", "discount_amount"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in ("created_at", "updated_at"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        if "status" in df.columns:
            df["status"] = df["status"].astype(str).str.lower().str.strip()

        if "currency" in df.columns:
            df["currency"] = df["currency"].astype(str).str.upper().str.strip()

        for col in ("shipping_country", "billing_country"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().str.strip()

        return df

    def _fill_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace NaN with sensible defaults to avoid downstream issues."""
        if "discount_amount" in df.columns:
            df["discount_amount"] = df["discount_amount"].fillna(0.0)

        if "currency" in df.columns:
            df["currency"] = df["currency"].fillna("EUR").replace("NAN", "EUR")

        if "status" in df.columns:
            df["status"] = df["status"].fillna("unknown")

        for col in ("shipping_country", "billing_country"):
            if col in df.columns:
                df[col] = df[col].fillna("UNKNOWN").replace("NAN", "UNKNOWN")

        if "items" in df.columns:
            df["items"] = df["items"].apply(
                lambda x: x if isinstance(x, list) else []
            )

        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate order_id entries, keeping the record with the
        most recent updated_at timestamp.
        """
        if "order_id" not in df.columns:
            log.warning("order_id column missing — skipping deduplication")
            return df

        before = len(df)

        if "updated_at" in df.columns:
            df = (
                df.sort_values("updated_at", ascending=False, na_position="last")
                .drop_duplicates(subset=["order_id"], keep="first")
                .reset_index(drop=True)
            )
        else:
            df = df.drop_duplicates(subset=["order_id"], keep="last").reset_index(
                drop=True
            )

        dropped = before - len(df)
        if dropped:
            log.info("Duplicates removed", dropped=dropped, remaining=len(df))

        return df

    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute net_amount, order_value_eur, is_international, item_count."""
        # net_amount = amount - discount
        amount = df.get("amount", pd.Series(dtype=float))
        discount = df.get("discount_amount", pd.Series(0.0, index=df.index))
        df["net_amount"] = (amount - discount).clip(lower=0)

        # order_value_eur — convert using static FX rates
        if "currency" in df.columns:
            df["order_value_eur"] = df.apply(
                lambda row: self._to_eur(row.get("net_amount", 0), row.get("currency", "EUR")),
                axis=1,
            ).round(2)
        else:
            df["order_value_eur"] = df["net_amount"].round(2)

        # is_international — True if shipping country differs from domestic
        if "shipping_country" in df.columns:
            df["is_international"] = df["shipping_country"] != self.domestic_country
        else:
            df["is_international"] = False

        # item_count — number of line items in the order
        if "items" in df.columns:
            df["item_count"] = df["items"].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
        else:
            df["item_count"] = 0

        return df

    def _add_partition_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add year/month/day columns derived from created_at for ADLS partitioning."""
        ref_col = "created_at" if "created_at" in df.columns else "updated_at"

        if ref_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[ref_col]):
            df["order_date"] = df[ref_col].dt.date.astype(str)
            df["year"] = df[ref_col].dt.year.astype(str)
            df["month"] = df[ref_col].dt.month.astype(str).str.zfill(2)
            df["day"] = df[ref_col].dt.day.astype(str).str.zfill(2)
        else:
            today = datetime.utcnow()
            df["order_date"] = today.strftime("%Y-%m-%d")
            df["year"] = str(today.year)
            df["month"] = today.strftime("%m")
            df["day"] = today.strftime("%d")

        return df

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop internal columns (items list) and reorder."""
        drop_cols = ["items"]
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])
        return df

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _to_eur(self, amount: float, currency: str) -> float:
        """Convert an amount in the given currency to EUR."""
        rate = self.fx_rates.get(currency, None)
        if rate is None:
            log.warning("Unknown currency, defaulting rate to 1.0", currency=currency)
            rate = 1.0
        return round(amount * rate, 4)
