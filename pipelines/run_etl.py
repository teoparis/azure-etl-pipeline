"""
ETL pipeline entry point.

Usage:
    python pipelines/run_etl.py --source rest --date 2024-01-15 --dry-run
    python pipelines/run_etl.py --source jdbc --date 2024-01-15

Arguments:
    --source   Data source type: rest | jdbc
    --date     Partition date in YYYY-MM-DD format (default: today UTC)
    --dry-run  Extract and transform but skip the ADLS load
    --entity   Entity/table to process (default: orders)
"""

import argparse
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Allow running from repo root: python pipelines/run_etl.py
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from dotenv import load_dotenv

from src.extractors.rest_extractor import RESTExtractor
from src.extractors.jdbc_extractor import JDBCExtractor
from src.transformers.order_transformer import OrderTransformer
from src.loaders.adls_loader import ADLSLoader
from src.utils.azure_client import AzureClientFactory
from src.utils.logger import configure_logging, get_logger

load_dotenv()

log = get_logger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load and return the pipeline YAML config."""
    with open(config_path) as f:
        raw = f.read()

    # Naive env var substitution for ${VAR} and ${VAR:-default}
    import re

    def _replace(match):
        key, *rest = match.group(1).split(":-")
        default = rest[0] if rest else ""
        return os.environ.get(key, default)

    raw = re.sub(r"\$\{([^}]+)\}", _replace, raw)
    return yaml.safe_load(raw)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Azure ETL Pipeline runner")
    parser.add_argument(
        "--source",
        choices=["rest", "jdbc"],
        required=True,
        help="Extraction source type",
    )
    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Partition date (YYYY-MM-DD), default: today UTC",
    )
    parser.add_argument(
        "--entity",
        default="orders",
        help="Entity/table name to process (default: orders)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform only, do not write to ADLS",
    )
    return parser.parse_args()


def extract_rest(config: dict, date: str, entity: str) -> list[dict]:
    """Extract records from REST API for the given date."""
    source_cfg = config["sources"]["rest_api"]
    extractor = RESTExtractor.from_env(source_cfg)

    endpoint = source_cfg["endpoints"].get(entity, f"/{entity}")
    params = {"updated_after": date, "updated_before": f"{date}T23:59:59Z"}

    log.info("REST extraction starting", endpoint=endpoint, date=date)
    records = extractor.extract(endpoint=endpoint, params=params)
    log.info("REST extraction done", records=len(records))
    return records


def extract_jdbc(config: dict, date: str, entity: str):
    """Extract records from SQL Server for the given date."""
    source_cfg = config["sources"]["jdbc"]

    query = f"""
        SELECT *
        FROM dbo.{entity}
        WHERE CAST(updated_at AS DATE) = ?
    """
    log.info("JDBC extraction starting", entity=entity, date=date)

    with JDBCExtractor.from_env(source_cfg) as extractor:
        df = extractor.extract_query(query=query, params=(date,))

    log.info("JDBC extraction done", rows=len(df))
    return df


def run(args: argparse.Namespace, config: dict) -> None:
    """Main ETL pipeline execution."""
    log.info(
        "Pipeline started",
        source=args.source,
        date=args.date,
        entity=args.entity,
        dry_run=args.dry_run,
    )

    # ── Extract ───────────────────────────────────────────────────────────────
    if args.source == "rest":
        raw_data = extract_rest(config, args.date, args.entity)
    else:
        raw_data = extract_jdbc(config, args.date, args.entity)

    if not hasattr(raw_data, "__len__") or len(raw_data) == 0:
        log.warning("No data extracted — pipeline exiting early", date=args.date)
        return

    # ── Transform ─────────────────────────────────────────────────────────────
    transformer = OrderTransformer()
    df = transformer.transform(raw_data)

    log.info(
        "Transformation complete",
        rows=len(df),
        columns=list(df.columns),
        date=args.date,
    )

    if args.dry_run:
        log.info(
            "DRY RUN — skipping ADLS load",
            sample=df.head(3).to_dict(orient="records"),
        )
        print(df.head(10).to_string())
        return

    # ── Load ──────────────────────────────────────────────────────────────────
    pipeline_cfg = config["pipeline"]
    adls_cfg = config["adls"]
    azure_cfg = config["azure"]

    factory = AzureClientFactory.from_env()
    loader = ADLSLoader(
        client_factory=factory,
        container=azure_cfg["container"],
        base_path=adls_cfg["base_path"],
        compression=adls_cfg.get("compression", "snappy"),
        write_mode=adls_cfg.get("write_mode", "overwrite"),
        batch_size=pipeline_cfg.get("batch_size", 100_000),
    )

    paths = loader.load(df, entity=args.entity, date=args.date)
    log.info("Pipeline complete", files_written=len(paths), paths=paths)


def main() -> None:
    args = parse_args()
    config = load_config()

    configure_logging(
        log_level=config["pipeline"].get("log_level", "INFO"),
        log_dir=config["pipeline"].get("log_dir", "logs"),
    )

    try:
        run(args, config)
    except Exception as exc:
        log.exception("Pipeline failed with unhandled exception", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
