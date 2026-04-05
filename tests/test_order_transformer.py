"""
Tests for OrderTransformer.

Covers:
  - Column normalisation (aliases, lowercase)
  - Type casting (amounts, dates)
  - Null handling
  - Deduplication (keep latest updated_at)
  - Derived columns: net_amount, order_value_eur, is_international, item_count
  - Partition columns: year, month, day, order_date
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from src.transformers.order_transformer import OrderTransformer


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_orders() -> list[dict]:
    """Realistic e-commerce order records as returned by the REST extractor."""
    return [
        {
            "order_id": "ORD-1001",
            "customer_id": "CUST-42",
            "status": "completed",
            "currency": "EUR",
            "amount": 129.99,
            "discount_amount": 10.00,
            "shipping_country": "IT",
            "billing_country": "IT",
            "created_at": "2024-01-15T08:30:00Z",
            "updated_at": "2024-01-15T09:00:00Z",
            "items": [{"sku": "SKU-A"}, {"sku": "SKU-B"}],
        },
        {
            "order_id": "ORD-1002",
            "customer_id": "CUST-99",
            "status": "shipped",
            "currency": "USD",
            "amount": 250.00,
            "discount_amount": None,   # null discount
            "shipping_country": "DE",
            "billing_country": "IT",
            "created_at": "2024-01-15T10:00:00Z",
            "updated_at": "2024-01-15T11:00:00Z",
            "items": [{"sku": "SKU-C"}],
        },
        {
            "order_id": "ORD-1003",
            "customer_id": "CUST-77",
            "status": "pending",
            "currency": "GBP",
            "amount": 85.50,
            "discount_amount": 5.00,
            "shipping_country": "GB",
            "billing_country": "GB",
            "created_at": "2024-01-15T12:00:00Z",
            "updated_at": "2024-01-15T12:30:00Z",
            "items": [],
        },
        # Duplicate of ORD-1001 with a more recent updated_at — should survive dedup
        {
            "order_id": "ORD-1001",
            "customer_id": "CUST-42",
            "status": "refunded",
            "currency": "EUR",
            "amount": 129.99,
            "discount_amount": 10.00,
            "shipping_country": "IT",
            "billing_country": "IT",
            "created_at": "2024-01-15T08:30:00Z",
            "updated_at": "2024-01-15T14:00:00Z",  # newer
            "items": [{"sku": "SKU-A"}, {"sku": "SKU-B"}],
        },
        {
            "order_id": "ORD-1004",
            "customer_id": "CUST-11",
            "status": "Completed",          # mixed case — should be normalised
            "currency": "usd",              # lowercase — should be uppercased
            "amount": "320.00",             # string amount — should be cast
            "discount_amount": 0,
            "shipping_country": "us",       # lowercase country
            "billing_country": "us",
            "created_at": "2024-01-15T15:00:00Z",
            "updated_at": "2024-01-15T15:05:00Z",
            "items": None,                  # null items
        },
    ]


@pytest.fixture
def transformer() -> OrderTransformer:
    return OrderTransformer(domestic_country="IT")


@pytest.fixture
def transformed(transformer, raw_orders) -> pd.DataFrame:
    return transformer.transform(raw_orders)


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestColumnNormalization:
    def test_order_id_present(self, transformed):
        assert "order_id" in transformed.columns

    def test_status_lowercase(self, transformed):
        """Status values must be normalised to lowercase."""
        assert all(transformed["status"].str.islower())

    def test_currency_uppercase(self, transformed):
        """Currency codes must be uppercase."""
        assert all(transformed["currency"].str.isupper())

    def test_shipping_country_uppercase(self, transformed):
        """Country codes must be uppercase."""
        assert all(transformed["shipping_country"].str.isupper())


class TestTypeCasting:
    def test_amount_is_float(self, transformed):
        """Amount must be numeric even if it came in as a string."""
        assert pd.api.types.is_float_dtype(transformed["amount"])

    def test_discount_amount_is_float(self, transformed):
        assert pd.api.types.is_float_dtype(transformed["discount_amount"])

    def test_created_at_is_datetime(self, transformed):
        assert pd.api.types.is_datetime64_any_dtype(transformed["created_at"])

    def test_updated_at_is_datetime(self, transformed):
        assert pd.api.types.is_datetime64_any_dtype(transformed["updated_at"])


class TestNullHandling:
    def test_null_discount_replaced_with_zero(self, transformed):
        """Null discount_amount must become 0.0 not NaN."""
        assert transformed["discount_amount"].isna().sum() == 0

    def test_null_items_replaced_with_zero_count(self, transformed):
        """Orders with null items should have item_count = 0."""
        ord_1004 = transformed[transformed["order_id"] == "ORD-1004"]
        assert not ord_1004.empty
        assert int(ord_1004["item_count"].iloc[0]) == 0


class TestDeduplication:
    def test_dedup_removes_duplicate_order_id(self, transformed):
        """After dedup, each order_id should appear exactly once."""
        counts = transformed["order_id"].value_counts()
        assert (counts > 1).sum() == 0, "Duplicates found after deduplication"

    def test_dedup_keeps_most_recent_updated_at(self, transformed):
        """For ORD-1001, the record with updated_at=14:00 should be kept."""
        ord_1001 = transformed[transformed["order_id"] == "ORD-1001"]
        assert not ord_1001.empty
        kept = ord_1001.iloc[0]
        assert kept["status"] == "refunded", (
            f"Expected status 'refunded' (most recent), got '{kept['status']}'"
        )

    def test_output_row_count(self, raw_orders, transformed):
        """5 raw records with 1 duplicate → 4 unique orders."""
        assert len(transformed) == 4


class TestDerivedColumns:
    def test_net_amount_equals_amount_minus_discount(self, transformed):
        """net_amount must be amount - discount_amount."""
        row = transformed[transformed["order_id"] == "ORD-1001"].iloc[0]
        expected = round(row["amount"] - row["discount_amount"], 10)
        assert abs(row["net_amount"] - expected) < 0.001

    def test_net_amount_is_non_negative(self, transformed):
        """net_amount should never be negative (clipped at 0)."""
        assert (transformed["net_amount"] >= 0).all()

    def test_order_value_eur_domestic(self, transformed):
        """EUR order should have order_value_eur ≈ net_amount."""
        row = transformed[transformed["order_id"] == "ORD-1001"].iloc[0]
        assert abs(row["order_value_eur"] - row["net_amount"]) < 0.01

    def test_order_value_eur_usd_conversion(self, transformed):
        """USD order should be converted to EUR (rate 0.92)."""
        row = transformed[transformed["order_id"] == "ORD-1002"].iloc[0]
        # net_amount = 250 - 0 = 250, EUR = 250 * 0.92 = 230.0
        assert abs(row["order_value_eur"] - 230.0) < 0.5

    def test_is_international_domestic(self, transformed):
        """Italian domestic order must not be flagged as international."""
        row = transformed[transformed["order_id"] == "ORD-1001"].iloc[0]
        assert row["is_international"] is False or row["is_international"] == False

    def test_is_international_foreign(self, transformed):
        """German shipping order must be flagged as international."""
        row = transformed[transformed["order_id"] == "ORD-1002"].iloc[0]
        assert row["is_international"] is True or row["is_international"] == True

    def test_item_count_correct(self, transformed):
        """ORD-1001 has 2 items, ORD-1003 has 0 items."""
        ord_1001 = transformed[transformed["order_id"] == "ORD-1001"].iloc[0]
        ord_1003 = transformed[transformed["order_id"] == "ORD-1003"].iloc[0]
        assert int(ord_1001["item_count"]) == 2
        assert int(ord_1003["item_count"]) == 0


class TestPartitionColumns:
    def test_year_column_present(self, transformed):
        assert "year" in transformed.columns

    def test_month_column_zero_padded(self, transformed):
        """Month must be zero-padded (e.g. '01' not '1')."""
        assert all(transformed["month"].str.len() == 2)

    def test_day_column_zero_padded(self, transformed):
        assert all(transformed["day"].str.len() == 2)

    def test_order_date_format(self, transformed):
        """order_date must be YYYY-MM-DD string."""
        import re
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        assert all(transformed["order_date"].apply(lambda d: bool(pattern.match(str(d)))))

    def test_partition_values_for_jan15(self, transformed):
        """Records with created_at in 2024-01-15 should have year=2024, month=01, day=15."""
        row = transformed[transformed["order_id"] == "ORD-1001"].iloc[0]
        assert row["year"] == "2024"
        assert row["month"] == "01"
        assert row["day"] == "15"


class TestEdgeCases:
    def test_empty_input_returns_empty_dataframe(self, transformer):
        result = transformer.transform([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_dataframe_input_accepted(self, transformer, raw_orders):
        """Transformer must accept a DataFrame as input, not just a list."""
        df_input = pd.DataFrame(raw_orders)
        result = transformer.transform(df_input)
        assert len(result) == 4
