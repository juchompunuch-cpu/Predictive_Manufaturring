# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze_Predictive_v2 — Manufacturing Defect Dataset (Bronze Layer)
# MAGIC
# MAGIC **สร้างใหม่จาก `Bronze_Predictive` (ไม่แก้ไฟล์เดิม)** — เขียนลงตารางใหม่ `..._v2` เพื่อให้ย้อนตรวจสอบตารางเดิมได้
# MAGIC
# MAGIC | # | ประเด็นใน notebook เดิม | หลักการ Bronze | สิ่งที่แก้ใน v2 |
# MAGIC |---|---|---|---|
# MAGIC | 1 | ใช้ `inferSchema=true` ตอนเขียนตาราง | Bronze = เก็บข้อมูล **ดิบตามต้นฉบับ** ไม่ตีความชนิดข้อมูล (type enforcement เป็นหน้าที่ Silver) | อ่านทุกคอลัมน์เป็น `STRING` ด้วย schema ที่ประกาศชัดเจน |
# MAGIC | 2 | ไม่มี ingestion metadata | Bronze ต้องตอบได้ว่าข้อมูลมาจากไฟล์ไหน เข้ามาเมื่อไร รอบไหน | เพิ่ม `_source_file`, `_source_file_modified_at`, `_ingested_at`, `_bronze_batch_id` |
# MAGIC | 3 | ไม่ตรวจ header/schema ของไฟล์ | ต้องจับ schema drift ตั้งแต่ทางเข้า | ตรวจ header เทียบ `EXPECTED_COLUMNS` ก่อนโหลด |
# MAGIC | 4 | แถวที่ format เสียจะหายเงียบ ๆ | ต้องเก็บ record ที่อ่านไม่ได้ไว้ตรวจสอบ | ใช้ `PERMISSIVE` + `_corrupt_record` |
# MAGIC | 5 | ไม่มี reconciliation | จำนวนแถวต้องตรงกับไฟล์ต้นทาง | นับบรรทัดในไฟล์เทียบกับจำนวนแถวในตาราง |
# MAGIC | 6 | ไม่มี log การโหลด | ETL Monitoring | append ผลทุกรอบลง `ingestion_log` |
# MAGIC | 7 | cell 1 และ cell 3 อ่านไฟล์ซ้ำกัน 2 รอบ ด้วย option ต่างกัน | 1 source → 1 วิธีอ่าน | อ่านครั้งเดียวด้วย reader function เดียว |
# MAGIC | 8 | ไม่มี table/column comment | Data catalog / data dictionary | เพิ่ม `COMMENT` ให้ตาราง |
# MAGIC | 9 | (v2.1) path/ตารางถูก hard-code | ต้องทดสอบกับข้อมูลสกปรกได้โดยไม่ทับตารางจริง | รับ **parameter ผ่าน widgets** (ค่า default = ตารางจริง) → `DQ_Test_Dirty_Data` เรียกใช้ notebook นี้ได้ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config (widgets: ค่า default = production)
# MAGIC รันตามปกติ (Run all) จะใช้ค่า default ทั้งหมด · notebook ทดสอบส่งค่าใหม่มาผ่าน `dbutils.notebook.run(...)`

# COMMAND ----------

import os
import uuid
from datetime import datetime, timezone
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG       = "predictive_manufacring_defect"      # ใช้ชื่อเดิมใน Unity Catalog (สะกดตามของเดิม)
BRONZE_SCHEMA = f"{CATALOG}.bronze_preditive"

dbutils.widgets.text("source_file",  "/Volumes/predictive_manufacring_defect/bronze_preditive/bronze_predic/manufacturing_defect_dataset.csv")
dbutils.widgets.text("bronze_table", f"{BRONZE_SCHEMA}.manufacturing_defect_dataset_v2")
dbutils.widgets.text("ingest_log",   f"{BRONZE_SCHEMA}.ingestion_log")

SOURCE_FILE  = dbutils.widgets.get("source_file")
VOLUME_DIR   = os.path.dirname(SOURCE_FILE)
BRONZE_TABLE = dbutils.widgets.get("bronze_table")
INGEST_LOG   = dbutils.widgets.get("ingest_log")

# คอลัมน์ที่ต้องมีในไฟล์ต้นทาง (ตามลำดับ)
EXPECTED_COLUMNS = [
    "ProductionVolume", "ProductionCost", "SupplierQuality", "DeliveryDelay",
    "DefectRate", "QualityScore", "MaintenanceHours", "DowntimePercentage",
    "InventoryTurnover", "StockoutRate", "WorkerProductivity", "SafetyIncidents",
    "EnergyConsumption", "EnergyEfficiency", "AdditiveProcessTime",
    "AdditiveMaterialCost", "DefectStatus",
]

bronze_batch_id = str(uuid.uuid4())
run_started_at  = datetime.now(timezone.utc)
print("Source file    :", SOURCE_FILE)
print("Bronze table   :", BRONZE_TABLE)
print("Bronze batch id:", bronze_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ตรวจไฟล์ต้นทาง (existence + header / schema drift)

# COMMAND ----------

files = [f for f in dbutils.fs.ls(VOLUME_DIR) if f.name.endswith(".csv")]
display(spark.createDataFrame([(f.name, f.size, f.modificationTime) for f in files],
                              "file_name string, size_bytes long, modification_time_ms long"))

if os.path.basename(SOURCE_FILE) not in [f.name for f in files]:
    raise FileNotFoundError(f"ไม่พบไฟล์ต้นทาง: {SOURCE_FILE}")

# อ่านเฉพาะบรรทัด header แบบ text (ไม่ตีความข้อมูล)
raw_lines    = spark.read.text(SOURCE_FILE)
header_line  = raw_lines.first()["value"].lstrip("﻿").strip()
file_columns = [c.strip() for c in header_line.split(",")]

missing_cols    = [c for c in EXPECTED_COLUMNS if c not in file_columns]
unexpected_cols = [c for c in file_columns if c not in EXPECTED_COLUMNS]
print("Header columns :", len(file_columns))
print("Missing        :", missing_cols)
print("Unexpected     :", unexpected_cols)

if missing_cols:
    raise ValueError(f"Schema drift: ไฟล์ขาดคอลัมน์ {missing_cols}")
if file_columns != EXPECTED_COLUMNS:
    print("WARNING: ลำดับ/ชุดคอลัมน์ต่างจากที่คาดไว้ — จะอ่านตามชื่อ header ของไฟล์")

# จำนวนแถวข้อมูลในไฟล์ (ไม่นับ header และบรรทัดว่าง) ใช้ทำ reconciliation
source_line_count = raw_lines.filter(F.length(F.trim("value")) > 0).count() - 1
print("Data lines in source file:", source_line_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract — อ่านเป็น STRING ทั้งหมด (raw fidelity) + เก็บแถวที่เสีย

# COMMAND ----------

raw_schema = StructType(
    [StructField(c, StringType(), True) for c in file_columns]
    + [StructField("_corrupt_record", StringType(), True)]
)

df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("encoding", "UTF-8")
    .schema(raw_schema)
    .load(SOURCE_FILE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. เพิ่ม Ingestion Metadata (lineage ระดับไฟล์)

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_source_file",             F.col("_metadata.file_path"))
    .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at",             F.current_timestamp())
    .withColumn("_bronze_batch_id",         F.lit(bronze_batch_id))
)

df_bronze.printSchema()
display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load — เขียน Delta Table (overwrite = idempotent, รันซ้ำไม่ซ้ำข้อมูล)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_TABLE.rsplit('.', 1)[0]}")
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

spark.sql(f"""
COMMENT ON TABLE {BRONZE_TABLE} IS
'Bronze (raw) copy of {os.path.basename(SOURCE_FILE)}. All business columns stored as STRING exactly as in source. Metadata: _source_file, _source_file_modified_at, _ingested_at, _bronze_batch_id. Malformed rows kept in _corrupt_record.'
""")
print("Written:", BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reconciliation & Bronze Profiling
# MAGIC (อ่านกลับจาก Delta table — การนับ `_corrupt_record` ตรงจากไฟล์ CSV ไม่ได้รับอนุญาตใน Spark)

# COMMAND ----------

bronze_tbl   = spark.table(BRONZE_TABLE)
bronze_rows  = bronze_tbl.count()
corrupt_rows = bronze_tbl.filter(F.col("_corrupt_record").isNotNull()).count()

print(f"Source data lines : {source_line_count}")
print(f"Bronze rows       : {bronze_rows}")
print(f"Corrupt rows      : {corrupt_rows}")

reconcile_status = "PASS" if bronze_rows == source_line_count else "FAIL"
print("Reconciliation    :", reconcile_status)

# Profiling: NULL / empty-string ต่อคอลัมน์ (ยังไม่แก้ — แค่รายงาน, การแก้อยู่ใน Silver)
profile = bronze_tbl.select(
    [F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c) for c in file_columns]
)
print("NULL/empty values per column (raw):")
display(profile)

print("Distinct values of DefectStatus (raw):")
display(bronze_tbl.groupBy("DefectStatus").count().orderBy("DefectStatus"))

if corrupt_rows > 0:
    display(bronze_tbl.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingestion Log (ETL Monitoring)

# COMMAND ----------

log_row = [(
    bronze_batch_id, SOURCE_FILE, BRONZE_TABLE,
    int(source_line_count), int(bronze_rows), int(corrupt_rows),
    reconcile_status, run_started_at, datetime.now(timezone.utc),
)]
log_schema = """batch_id string, source_file string, target_table string,
                source_rows long, loaded_rows long, corrupt_rows long,
                status string, started_at timestamp, finished_at timestamp"""

spark.createDataFrame(log_row, log_schema).write.format("delta").mode("append").saveAsTable(INGEST_LOG)
display(spark.table(INGEST_LOG).orderBy(F.desc("finished_at")).limit(10))

if reconcile_status != "PASS":
    raise ValueError("Bronze reconciliation failed: จำนวนแถวไม่ตรงกับไฟล์ต้นทาง")
print("Bronze layer: PASS")
dbutils.notebook.exit(f"PASS|rows={bronze_rows}|corrupt={corrupt_rows}")