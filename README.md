# Azure ETL Pipeline

Production-grade ETL pipeline on Azure: REST and JDBC ingestion, order data transformation, ADLS Gen2 load, orchestrated via Azure Data Factory.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Azure Data Factory                           │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────┐    │
│  │  Lookup      │───>│  Copy        │───>│  StoredProcedure   │    │
│  │  (watermark) │    │  Activity    │    │  (update watermark)│    │
│  └──────────────┘    └──────┬───────┘    └────────────────────┘    │
│                             │                                       │
└─────────────────────────────┼───────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
   ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
   │ REST API    │    │ SQL / JDBC  │    │ ADLS Gen2   │
   │ (orders)   │    │ (customers) │    │ (parquet)   │
   └─────────────┘    └─────────────┘    └─────────────┘
          │                   │                   ▲
          └─────────┬─────────┘                   │
                    │                             │
          ┌─────────▼─────────┐                   │
          │  OrderTransformer │───────────────────┘
          │  (pandas)         │
          └───────────────────┘
```

## Tech Stack

| Component         | Technology                        |
|-------------------|-----------------------------------|
| Orchestration     | Azure Data Factory                |
| Storage           | Azure Data Lake Storage Gen2      |
| Compute           | Python 3.11 (local / ADF self-hosted IR) |
| Auth              | Azure DefaultAzureCredential      |
| Transformation    | pandas 2.x                        |
| DB Connectivity   | pyodbc (SQL Server / Azure SQL)   |
| HTTP Client       | requests + tenacity (retry)       |
| Logging           | loguru                            |
| Config            | PyYAML + python-dotenv            |
| Serialization     | Apache Parquet (via pyarrow)      |

## Project Structure

```
azure-etl-pipeline/
├── config/
│   └── config.yaml             # Pipeline configuration
├── src/
│   ├── extractors/
│   │   ├── rest_extractor.py   # REST API extractor with pagination & retry
│   │   └── jdbc_extractor.py   # JDBC/SQL extractor with chunked reads
│   ├── transformers/
│   │   └── order_transformer.py# Normalisation, dedup, derived columns
│   ├── loaders/
│   │   └── adls_loader.py      # ADLS Gen2 loader (parquet, partitioned)
│   └── utils/
│       ├── azure_client.py     # Azure SDK client factory
│       └── logger.py           # Structured loguru logger
├── pipelines/
│   └── run_etl.py              # CLI entry point
├── adf/
│   └── pipeline_orders_ingestion.json  # ADF pipeline definition
├── tests/
│   └── test_order_transformer.py
├── .env.example
├── requirements.txt
└── README.md
```

## Local Setup

```bash
# 1. Clone and create virtualenv
git clone https://github.com/teoparis/azure-etl-pipeline.git
cd azure-etl-pipeline
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your Azure credentials

# 4. Run the pipeline (dry-run first)
python pipelines/run_etl.py --source rest --date 2024-01-15 --dry-run

# 5. Run with actual load
python pipelines/run_etl.py --source rest --date 2024-01-15

# 6. Run tests
pytest tests/ -v
```

## Environment Variables

| Variable                        | Description                                      | Required |
|---------------------------------|--------------------------------------------------|----------|
| `AZURE_TENANT_ID`               | Azure AD tenant ID                               | Yes      |
| `AZURE_CLIENT_ID`               | Service principal client ID                      | Yes      |
| `AZURE_CLIENT_SECRET`           | Service principal client secret                  | Yes      |
| `AZURE_STORAGE_ACCOUNT`         | ADLS Gen2 storage account name                   | Yes      |
| `AZURE_STORAGE_CONTAINER`       | Default container name                           | Yes      |
| `AZURE_STORAGE_CONNECTION_STRING` | Alternative to SP auth (for dev/test)          | No       |
| `REST_API_BASE_URL`             | Base URL of the orders REST API                  | Yes      |
| `REST_API_TOKEN`                | Bearer token for REST API auth                   | Yes      |
| `JDBC_HOST`                     | SQL Server hostname                              | Yes      |
| `JDBC_PORT`                     | SQL Server port (default 1433)                   | No       |
| `JDBC_DATABASE`                 | Database name                                    | Yes      |
| `JDBC_USERNAME`                 | DB username                                      | Yes      |
| `JDBC_PASSWORD`                 | DB password                                      | Yes      |
| `LOG_LEVEL`                     | Logging level (DEBUG/INFO/WARNING)               | No       |

## ADF Deployment

Import `adf/pipeline_orders_ingestion.json` into your Azure Data Factory instance via the ADF Studio UI or using the Azure CLI:

```bash
az datafactory pipeline create \
  --resource-group <rg> \
  --factory-name <adf-name> \
  --name orders_ingestion \
  --pipeline @adf/pipeline_orders_ingestion.json
```

## Watermark Pattern

The pipeline implements a high-watermark pattern to support incremental loads:

1. **LookupActivity** reads the last processed `updated_at` timestamp from a control table
2. **CopyActivity** fetches only records with `updated_at > watermark`
3. **StoredProcedureActivity** updates the watermark to `MAX(updated_at)` of the current batch

## License

MIT
