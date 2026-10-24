# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze_AutoLoader — Incremental Bronze ด้วย Auto Loader (ใช้ใน Databricks Workflow)
# MAGIC
# MAGIC **แนวทาง C (แบบผสม):** Bronze รับเฉพาะ **ไฟล์ใหม่** ที่วางใน `landing/` แบบ append → Silver / Gold / Dashboard คำนวณใหม่จาก Bronze ทั้งหมด (ข้อมูลหลักพันแถว คำนวณใหม่ได้เร็วและผลตรงกับรายงานเสมอ)
# MAGIC
# MAGIC | หัวข้อ | Bronze_Predictive_v2 (batch) | Bronze_AutoLoader (incremental) |
# MAGIC |---|---|---|
# MAGIC | อ่านไฟล์ | อ่าน 1 ไฟล์ทั้งไฟล์ | Auto Loader (`cloudFiles`) อ่านเฉพาะไฟล์ใหม่ใน `landing/` (จำไฟล์ที่อ่านแล้วใน checkpoint) |
# MAGIC | เขียนตาราง | overwrite | **append** ลงตารางเดิม `bronze_preditive.manufacturing_defect_dataset_v2` → Silver/Gold/Alert ใช้ต่อได้ทันที |
# MAGIC | การ parse | ทุกคอลัมน์ STRING + `PERMISSIVE` + `_corrupt_record` | **เหมือนเดิมทุกอย่าง** (กลไก DQ ที่ทดสอบแล้วยังใช้ได้) |
# MAGIC | ตรวจ header | ก่อนโหลด | ตรวจ **ทีละไฟล์ใหม่** (schema drift → FAIL) |
# MAGIC | Reconciliation | ไฟล์ = ตาราง | **ทีละไฟล์**: บรรทัดในไฟล์ = แถวที่โหลดจากไฟล์นั้น |
# MAGIC | ส่งต่อ task ถัดไป | – | `dbutils.jobs.taskValues`: `new_rows`, `bronze_batch_id` → Job ใช้ตัดสินใจว่าจะรัน Silver ต่อหรือไม่ |
# MAGIC
# MAGIC ⚠️ ห้ามรัน `Bronze_Predictive_v2` ด้วยค่า default หลังเปิดใช้ Workflow (จะ overwrite ตาราง Bronze ที่ Auto Loader สะสมไว้) — ถ้าต้องการโหลดใหม่ทั้งหมด ให้ตั้ง widget `full_refresh = true`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config (widgets)

# COMMAND ----------

import uuid
from datetime import datetime, timezone
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG       = "predictive_manufacring_defect"
BRONZE_SCHEMA = f"{CATALOG}.bronze_preditive"

dbutils.widgets.text("landing_dir",  "/Volumes/predictive_manufacring_defect/bronze_preditive/landing/")
dbutils.widgets.text("bronze_table", f"{BRONZE_SCHEMA}.manufacturing_defect_dataset_v2")
dbutils.widgets.text("ingest_log",   f"{BRONZE_SCHEMA}.ingestion_log")
dbutils.widgets.text("checkpoint",   "/Volumes/predictive_manufacring_defect/bronze_preditive/checkpoints/bronze_autoloader")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])

LANDING_DIR  = dbutils.widgets.get("landing_dir")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")
INGEST_LOG   = dbutils.widgets.get("ingest_log")
CHECKPOINT   = dbutils.widgets.get("checkpoint")
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

EXPECTED_COLUMNS = [
    "ProductionVolume", "ProductionCost", "SupplierQuality", "DeliveryDelay",
    "DefectRate", "QualityScore", "MaintenanceHours", "DowntimePercentage",
    "InventoryTurnover", "StockoutRate", "WorkerProductivity", "SafetyIncidents",
    "EnergyConsumption", "EnergyEfficiency", "AdditiveProcessTime",
    "AdditiveMaterialCost", "DefectStatus",
]

spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE_SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE_SCHEMA}.checkpoints")

bronze_batch_id = str(uuid.uuid4())
run_started_at  = datetime.now(timezone.utc)
print("Landing dir    :", LANDING_DIR)
print("Bronze table   :", BRONZE_TABLE)
print("Bronze batch id:", bronze_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. (ถ้าเลือก) Full refresh — ล้างตารางและ checkpoint เพื่ออ่านทุกไฟล์ใน landing ใหม่

# COMMAND ----------

if FULL_REFRESH:
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
    dbutils.fs.rm(CHECKPOINT, recurse=True)
    print("Full refresh: dropped table and checkpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract — Auto Loader อ่านเฉพาะไฟล์ใหม่ (STRING ทั้งหมด + `_corrupt_record`)
# MAGIC `trigger(availableNow=True)` = อ่านไฟล์ใหม่ที่มีอยู่ตอนนี้ให้หมดแล้วหยุด (เหมาะกับ Job ที่ถูกปลุกเมื่อมีไฟล์มาใหม่)

# COMMAND ----------

raw_schema = StructType(
    [StructField(c, StringType(), True) for c in EXPECTED_COLUMNS]
    + [StructField("_corrupt_record", StringType(), True)]
)

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("encoding", "UTF-8")
    .option("pathGlobFilter", "*.csv")
    .schema(raw_schema)
    .load(LANDING_DIR)
    .withColumn("_source_file",             F.col("_metadata.file_path"))
    .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at",             F.current_timestamp())
    .withColumn("_bronze_batch_id",         F.lit(bronze_batch_id))
)

q = (
    stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)
q.awaitTermination()
print("Auto Loader finished")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ตรวจไฟล์ใหม่ทีละไฟล์: header (schema drift) + reconciliation

# COMMAND ----------

bronze_tbl  = spark.table(BRONZE_TABLE)
new_rows_df = bronze_tbl.filter(F.col("_bronze_batch_id") == bronze_batch_id)
new_rows    = new_rows_df.count()
files       = sorted(r["_source_file"] for r in new_rows_df.select("_source_file").distinct().collect())
print(f"New rows this run: {new_rows} | new files: {len(files)}")

log_rows, all_ok = [], True
for f in files:
    lines   = spark.read.text(f)
    header  = [c.strip() for c in lines.first()["value"].lstrip("﻿").strip().split(",")]
    n_lines = lines.filter(F.length(F.trim("value")) > 0).count() - 1
    loaded  = new_rows_df.filter(F.col("_source_file") == f).count()
    corrupt = new_rows_df.filter((F.col("_source_file") == f) & F.col("_corrupt_record").isNotNull()).count()
    if header != EXPECTED_COLUMNS:
        status = "SCHEMA_DRIFT"
    elif n_lines != loaded:
        status = "FAIL"
    else:
        status = "PASS"
    all_ok &= status == "PASS"
    log_rows.append((bronze_batch_id, f, BRONZE_TABLE, int(n_lines), int(loaded), int(corrupt),
                     status, run_started_at, datetime.now(timezone.utc)))
    print(f"  {f.split('/')[-1]}: file={n_lines} loaded={loaded} corrupt={corrupt} → {status}")

if not log_rows:
    log_rows.append((bronze_batch_id, None, BRONZE_TABLE, 0, 0, 0, "NO_NEW_FILES",
                     run_started_at, datetime.now(timezone.utc)))

log_schema = """batch_id string, source_file string, target_table string,
                source_rows long, loaded_rows long, corrupt_rows long,
                status string, started_at timestamp, finished_at timestamp"""
spark.createDataFrame(log_rows, log_schema).write.format("delta").mode("append").saveAsTable(INGEST_LOG)
display(spark.table(INGEST_LOG).orderBy(F.desc("finished_at")).limit(10))

spark.sql(f"""COMMENT ON TABLE {BRONZE_TABLE} IS
'Bronze (raw, append-only via Auto Loader) of manufacturing defect CSV files in landing volume. All business columns STRING. Metadata: _source_file, _source_file_modified_at, _ingested_at, _bronze_batch_id. Malformed rows kept in _corrupt_record.'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ส่งค่าให้ task ถัดไป (Job) + Gate

# COMMAND ----------

total_rows = spark.table(BRONZE_TABLE).count()
print(f"Bronze total rows: {total_rows}")

dbutils.jobs.taskValues.set(key="new_rows", value=int(new_rows))
dbutils.jobs.taskValues.set(key="bronze_batch_id", value=bronze_batch_id)

if not all_ok:
    raise ValueError("Bronze AutoLoader: schema drift หรือ reconciliation ไม่ผ่าน — ดู ingestion_log")
print("Bronze AutoLoader: PASS")
dbutils.notebook.exit(f"PASS|new_rows={new_rows}|total={total_rows}")