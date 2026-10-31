# Changelog

## v2.1
- **Gold:** added a threshold-scan step before building the marts. Each variable's cut-off is chosen from the data's actual change point (each side ≥ 5% of batches; key factor if gap ≥ 15 pp). The thresholds changed from the previous version (maintenance > 11, volume > 700, quality score < 80) to **maintenance > 10, volume > 800, quality score < 75**. Each variable is now tagged as Controllable or Outcome.
- **Silver:** `source_file_modified_at` is now carried from Bronze into Silver lineage.

## Later updates
- **ML:** added `ML_Predictive` (Logistic Regression, Gradient Boosting, 4-rule baseline, MLflow + UC model alias `champion`) as the last Workflow task, plus a pre-production-only experiment.
- **Workflow v2:** `dq_test` now runs in parallel with `bronze_autoloader`. The DQ child-notebook timeout is 1,800 s. Alert tasks get retry 2 (60 s) and a 900 s timeout. `Reset_Demo` no longer waits for the Job to finish.

## v2.0
- **Bronze_Predictive_v2:** reads every column as STRING (no `inferSchema`) and adds ingestion metadata. Also adds header/schema-drift checks, PERMISSIVE mode with `_corrupt_record`, row reconciliation, `ingestion_log`, table comments and widget parameters (9 fixes from the original notebook).
- **Bronze_AutoLoader:** incremental ingestion from `landing/` with a checkpoint and `trigger(availableNow)`. Passes `new_rows` to the Job.
- **Silver_Predictive_v2:** snake_case naming, 10 placeholder patterns → NULL, `try_cast` and `record_id` SHA-256 dedup. Adds 30+ DQ rules, quarantine, lineage, a quality gate and 9 Delta CHECK constraints. Fixes the stockout naming/range (`stockout_pct` 0–100 → `stockout_rate` 0–1).
- **Gold_Predictive:** 6 domain tables with PK/FK (1:1), `batch_360` and analytic marts. Loads into Databricks Lakebase (PostgreSQL 17) via `pg8000` and `COPY FROM STDIN`, with 5 views and an integrity test (14/14).
- **DQ_Test_Dirty_Data:** 15 dirty cases + 5 reconciliation checks (19/19).
- **Automation:** `Setup_Workflow` (Jobs API 2.2, file-arrival trigger, 2 SQL Alerts, email on failure), `Demo_Drop_New_Batch` and `Reset_Demo`.

## v1.0
- Original Bronze / Silver / Gold notebooks (superseded by v2).
