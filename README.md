# Retail Lakehouse on Azure (Olist)

An end-to-end data engineering project on Azure using the public Olist Brazilian e-commerce dataset. It follows the medallion architecture (raw → bronze → silver → gold) with a metadata-driven design, data quality checks, and a quarantine table for bad rows.

**Status:** Phase 1 and Phase 2 complete. Orchestration with ADF, Gold layer, and audit logging are in progress.

## Architecture

```
Laptop ──► source (ADLS) ──ADF──► raw ──Databricks──► bronze ──► silver ──► gold
           simulated external      exact     CSV → Delta     typed, checked,   star schema
           system                  copy                      MERGE upserts     (planned)
                                                             └─ _quarantine (bad rows)

Azure SQL (ctl schema): pipeline_config + watermark_state  → drives the pipeline (metadata-driven)
```

| Layer | Container | Format | Purpose |
|---|---|---|---|
| Source | `source` | CSV | Simulated outside system, daily files |
| Raw | `raw` | CSV | Untouched copy, used for replay |
| Bronze | `bronze` | Delta | Same data as strings plus technical columns |
| Silver | `silver` | Delta | Typed, validated, deduplicated, upserted |
| Quarantine | `silver/_quarantine` | Delta | Rows that failed quality rules, with reasons |
| Gold | `gold` | Delta | Star schema (planned) |

## Tech stack

Azure Data Lake Storage Gen2, Azure Data Factory, Azure Databricks (Unity Catalog, Delta Lake, PySpark), Azure SQL Database, Python (pandas), Managed Identity / Access Connector for passwordless access.

## Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle: 9 CSV files (orders, items, payments, reviews, customers, products, sellers, geolocation, category translation).

**Important: the daily feed is simulated.** Olist is a static dump, so `data_prep/split_olist.py` creates a realistic daily feed:

- Everything before 1 June 2018 becomes one history load (folder date `2018-05-31`).
- 1–30 June 2018 become one folder per day (`ingestion_date=YYYY-MM-DD`, Hive style).
- Orders appear twice (a `created` row, then the final status on a later day), so MERGE has real updates. A `last_updated_ts` watermark column is added because Olist has none.
- Customer address changes on some days are **synthetic** and exist only to practise SCD Type 2.

## Design decisions

- **Raw is never edited.** If Silver logic has a bug, I fix it and replay from raw.
- **Metadata-driven.** `ctl.pipeline_config` holds table name, keys, load type, and watermark column. Adding a table means adding a row, not a new pipeline. Config and state (`ctl.watermark_state`) are separate tables so a config deployment cannot overwrite progress.
- **Idempotent loads.** Bronze and quarantine use `replaceWhere` on `_ingestion_date`; Silver uses MERGE. Re-running a day gives the same result with no duplicates.
- **Bad rows are quarantined, not fatal.** Casting uses `try_cast`, and each table has SQL quality rules. Failed rows are saved with the failed rule names and the original row as JSON.
- **Late data cannot overwrite newer data.** MERGE updates only when the incoming `last_updated_ts` is greater than or equal to the stored one.
- **Dedupe before MERGE**, because Delta MERGE fails when two source rows match one target row.
- **No secrets in notebooks.** Storage access uses managed identity; Databricks never needs the SQL password.

## Repository structure

```
├── data_prep/split_olist.py          # builds the simulated daily feed
├── sql/01_control_tables.sql         # ctl.pipeline_config, ctl.watermark_state
├── notebooks/
│   ├── 00_common.py                  # paths, schemas, quality rules
│   ├── 01_bronze_ingest.py           # raw CSV → Bronze Delta
│   ├── 02_silver_transform.py        # type cast → quality check → quarantine → MERGE
│   └── 03_dev_runner.py              # temporary stand-in for ADF orchestration
└── docs/screenshots/
```

## How to run

1. Download the Olist CSVs from Kaggle into `olist_raw/` and run `python data_prep/split_olist.py`.
2. Create the Azure resources (storage with hierarchical namespace, Databricks, Data Factory, Azure SQL) in one resource group and region.
3. Upload `source_out/` to the `source` container, run `sql/01_control_tables.sql`, and bulk copy `source` to `raw` with ADF.
4. In Databricks, set `STORAGE_ACCOUNT` in `00_common`, then run `03_dev_runner`.

## Results

Fill these in from your own runs.

| Check | Result |
|---|---|
| Bronze orders rows = rows in raw files | TODO |
| Silver orders rows = unique orders | TODO |
| Silver duplicates on `order_id` | 0 (TODO confirm) |
| Idempotency (same day run twice) | TODO |
| Quarantine demo (3 bad payment rows) | TODO |

Screenshots are in `docs/screenshots/`.

## Roadmap

- [x] Phase 1: Azure setup, simulated daily feed, control tables, ADF connections
- [x] Phase 2: Bronze and Silver notebooks, data quality, quarantine, tests
- [ ] Phase 3: Gold star schema, SCD Type 2 customer dimension, audit log table
- [ ] ADF orchestration driven by `ctl.pipeline_config`, daily triggers
- [ ] Key Vault, Git integration, CI/CD

## Notes

- The data is public and anonymised. Customer changes are simulated.
- All resources live in one resource group so they can be deleted together to stop costs.
