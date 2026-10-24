# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver_Predictive_v2 — Manufacturing Defect Dataset (Silver Layer)
# MAGIC
# MAGIC **สร้างใหม่ต่อยอดจาก `Test3_Silver` (ไม่แก้ไฟล์เดิม)** — อ่านจาก `bronze_preditive.manufacturing_defect_dataset_v2` และเขียนลงตาราง `..._v2`
# MAGIC
# MAGIC โครงหลักของ Test3 ถูกต้องตามหลัก Silver แล้ว (rename → normalize missing → type enforcement → dedup → DQ rules → quarantine → lineage → quality gate)
# MAGIC ส่วนที่ **เสริม/แก้** ใน v2:
# MAGIC
# MAGIC | # | ประเด็นใน Test3 | ผลกระทบ | แก้ใน v2 |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Bronze เดิมถูก `inferSchema` มาแล้ว → ขั้น 2 (placeholder→NULL) และ 3 (try_cast) **ไม่เคยทำงานจริง** ("No string columns found") | Silver ไม่ได้พิสูจน์ว่าจัดการข้อมูลสกปรกได้ | Bronze v2 เก็บเป็น STRING → ทุกขั้นทำงานจริง (พิสูจน์ด้วย `DQ_Test_Dirty_Data`) |
# MAGIC | 2 | `StockoutRate` ค่าจริง 0–0.10 (สัดส่วน) แต่ตั้งชื่อ `stockout_pct` และเช็คช่วง 0–100 | ชื่อสื่อความผิด + rule หลวมเกินไป 1000 เท่า | เปลี่ยนชื่อเป็น `stockout_rate` และเช็คช่วง 0–1 |
# MAGIC | 3 | `EnergyEfficiency` (0.1–0.5) เช็คแค่ ≥ 0 | ค่าผิด เช่น 45 ผ่านได้ | เช็คช่วง 0–1 |
# MAGIC | 4 | `maintenance_hours_per_week` ไม่มีเพดาน | 1 สัปดาห์มีแค่ 168 ชม. | เพิ่ม rule ≤ 168 |
# MAGIC | 5 | ไม่มี key ประจำแถว | Gold ต้องมี PK/FK ตามแผน RDBMS | เพิ่ม `record_id` = SHA-256 ของค่าธุรกิจ (deterministic, รันซ้ำได้ค่าเดิม) |
# MAGIC | 6 | แถวซ้ำถูก `dropDuplicates` ทิ้งเงียบ ๆ | ตรวจย้อนไม่ได้ว่าอะไรถูกตัด | ส่งแถวซ้ำเข้า quarantine พร้อมเหตุผล `exact duplicate` |
# MAGIC | 7 | lineage มีแค่ `source_table` | ย้อนไปถึงไฟล์/รอบ Bronze ไม่ได้ | ส่งต่อ `_source_file`, `_source_file_modified_at`, `_bronze_batch_id`, `_ingested_at` |
# MAGIC | 8 | ไม่มี constraint บนตาราง | ข้อมูลผิดอาจถูกเขียนทับภายหลังได้ | เพิ่ม Delta `CHECK` constraints + table comment |
# MAGIC | 9 | ไม่มี run log | ETL Monitoring (บทที่ 7.4 ในแผนรายงาน) | append row trace ลง `silver_predictive.dq_run_log` |
# MAGIC | 10 | ไม่มี profiling หลัง clean | ใช้เขียนบท Data Understanding | เพิ่ม summary statistics + class balance ของ target |
# MAGIC | 11 | (v2.1) ตารางถูก hard-code | ทดสอบข้อมูลสกปรกไม่ได้โดยไม่ทับตารางจริง | รับ parameter ผ่าน widgets (default = ตารางจริง) |

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. INIT (widgets: ค่า default = production)

# COMMAND ----------

import uuid
from datetime import datetime, timezone
import pyspark.sql.functions as F
from pyspark.sql.window import Window

CATALOG       = "predictive_manufacring_defect"
SILVER_SCHEMA = f"{CATALOG}.silver_predictive"

dbutils.widgets.text("source_table",     f"{CATALOG}.bronze_preditive.manufacturing_defect_dataset_v2")
dbutils.widgets.text("silver_table",     f"{SILVER_SCHEMA}.manufacturing_defect_dataset_v2")
dbutils.widgets.text("quarantine_table", f"{SILVER_SCHEMA}.manufacturing_defect_dataset_quarantine_v2")
dbutils.widgets.text("dq_run_log",       f"{SILVER_SCHEMA}.dq_run_log")

SOURCE_TABLE     = dbutils.widgets.get("source_table")
SILVER_TABLE     = dbutils.widgets.get("silver_table")
QUARANTINE_TABLE = dbutils.widgets.get("quarantine_table")
DQ_RUN_LOG       = dbutils.widgets.get("dq_run_log")

WRITE_MODE = "overwrite"
MISSING_VALUES = ["", "null", "none", "nan", "n/a", "na", "-", "?", "missing", "undefined"]

silver_batch_id = str(uuid.uuid4())
run_started_at  = datetime.now(timezone.utc)

ROW_TRACE = []
def trace(step, df):
    n = df.count()
    ROW_TRACE.append((step, n))
    print(f"[row trace] {step:<38} {n:>8}")
    return n

df_silver = spark.table(SOURCE_TABLE)
print(f"Source table: {SOURCE_TABLE}")
print(f"Silver table: {SILVER_TABLE}")
bronze_count = trace("0. bronze", df_silver)

# แถวที่ Bronze อ่านไม่ได้ (malformed) → quarantine ทันที
bronze_corrupt = df_silver.filter(F.col("_corrupt_record").isNotNull())
df_silver      = df_silver.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
corrupt_count  = bronze_corrupt.count()
print("Malformed rows from Bronze:", corrupt_count)
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Column Name Standardization

# COMMAND ----------

RENAME_MAP = {
    # 1. Production
    "ProductionVolume": "production_volume",
    "ProductionCost": "production_cost",
    # 2. Supply Chain
    "SupplierQuality": "supplier_quality_pct",
    "DeliveryDelay": "delivery_delay_days",
    # 3. Quality
    "DefectRate": "defect_rate",
    "QualityScore": "quality_score_pct",
    # 4. Maintenance
    "MaintenanceHours": "maintenance_hours_per_week",
    "DowntimePercentage": "downtime_pct",
    # 5. Inventory
    "InventoryTurnover": "inventory_turnover_ratio",
    "StockoutRate": "stockout_rate",                 # v2: สัดส่วน 0–1 ไม่ใช่ %
    # 6. Workforce & Safety
    "WorkerProductivity": "worker_productivity_pct",
    "SafetyIncidents": "safety_incidents_count",
    # 7. Energy
    "EnergyConsumption": "energy_consumption_kwh",
    "EnergyEfficiency": "energy_efficiency_factor",
    # 8. Additive Manufacturing
    "AdditiveProcessTime": "additive_process_time_hrs",
    "AdditiveMaterialCost": "additive_material_cost_usd",
    # 9. Target
    "DefectStatus": "is_high_defect",
}

# v2.1: ส่งต่อ _source_file_modified_at ด้วย (เดิมหายไประหว่าง Bronze → Silver)
BRONZE_META = {
    "_source_file": "source_file",
    "_source_file_modified_at": "source_file_modified_at",
    "_ingested_at": "bronze_ingested_at",
    "_bronze_batch_id": "bronze_batch_id",
}

missing_source_cols = sorted(set(RENAME_MAP) - set(df_silver.columns))
if missing_source_cols:
    raise ValueError("Missing required source columns: " + ", ".join(missing_source_cols))
print("Source schema validation: PASS")

df_silver = df_silver.select(
    *[F.col(o).alias(n) for o, n in RENAME_MAP.items()],
    *[F.col(o).alias(n) for o, n in BRONZE_META.items()],
)
BUSINESS_COLUMNS = list(RENAME_MAP.values())
print(df_silver.columns)
trace("1. after rename", df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Missing Value Normalization
# MAGIC Placeholder (เช่น "N/A", "null", "") → NULL — ใน v2 ขั้นนี้ทำงานจริงเพราะ Bronze เป็น STRING

# COMMAND ----------

string_cols = [c for c, t in df_silver.dtypes if t == "string" and c in BUSINESS_COLUMNS]
print("String business columns:", len(string_cols))

placeholder_counts = df_silver.select([
    F.sum(F.when(F.lower(F.trim(F.col(c))).isin(MISSING_VALUES), 1).otherwise(0)).alias(c)
    for c in string_cols
])
print("Placeholder missing values BEFORE normalization:")
display(placeholder_counts)

for c in string_cols:
    cleaned = F.trim(F.col(c))
    df_silver = df_silver.withColumn(
        c, F.when(F.lower(cleaned).isin(MISSING_VALUES), F.lit(None)).otherwise(cleaned)
    )

print("NULL count AFTER normalization:")
display(df_silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in BUSINESS_COLUMNS]))
trace("2. after missing normalization", df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Data Type Enforcement
# MAGIC - `try_cast` → ค่าที่แปลงไม่ได้เป็น NULL (ไม่ throw ใน ANSI mode) แล้ว flag ไว้
# MAGIC - integer: cast ผ่าน double (รองรับ "12.0") และ flag ถ้ามีเศษทศนิยม

# COMMAND ----------

EXPECTED_TYPES = {
    "production_volume": "int",
    "production_cost": "double",
    "supplier_quality_pct": "double",
    "delivery_delay_days": "int",
    "defect_rate": "double",
    "quality_score_pct": "double",
    "maintenance_hours_per_week": "int",
    "downtime_pct": "double",
    "inventory_turnover_ratio": "double",
    "stockout_rate": "double",
    "worker_productivity_pct": "double",
    "safety_incidents_count": "int",
    "energy_consumption_kwh": "double",
    "energy_efficiency_factor": "double",
    "additive_process_time_hrs": "double",
    "additive_material_cost_usd": "double",
    "is_high_defect": "int",
}

def try_cast(col_name, target_type):
    return F.expr(f"try_cast(`{col_name}` AS {target_type})")

cast_error_expressions = []
for c, target_type in EXPECTED_TYPES.items():
    as_double = try_cast(c, "double")
    cast_error_expressions.append(
        F.when(F.col(c).isNotNull() & as_double.isNull(), F.lit(f"{c}: invalid value for {target_type}"))
    )
    if target_type == "int":
        cast_error_expressions.append(
            F.when(as_double.isNotNull() & ~F.isnan(as_double) & (as_double != F.floor(as_double)),
                   F.lit(f"{c}: non-integer value for int"))
        )

df_silver = df_silver.withColumn("_dq_cast_errors", F.array_compact(F.array(*cast_error_expressions)))

for c, target_type in EXPECTED_TYPES.items():
    if target_type == "int":
        df_silver = df_silver.withColumn(c, try_cast(c, "double").cast("int"))
    else:
        df_silver = df_silver.withColumn(c, try_cast(c, target_type))

print("Rows with casting errors:", df_silver.filter(F.size("_dq_cast_errors") > 0).count())
df_silver.printSchema()
trace("3. after type enforcement", df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Record Key + Deduplication
# MAGIC - `record_id` = SHA-256 ของค่าธุรกิจทั้ง 17 คอลัมน์ → ใช้เป็น natural key ส่งต่อให้ Gold (PK/FK)
# MAGIC - แถวซ้ำ (record_id เดียวกัน) เก็บไว้ 1 แถว ที่เหลือส่ง quarantine พร้อมเหตุผล (ไม่ทิ้งเงียบ)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "record_id",
    F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in BUSINESS_COLUMNS]), 256),
)

w_dup = Window.partitionBy("record_id").orderBy(F.col("bronze_ingested_at"))
df_silver = df_silver.withColumn("_dup_rank", F.row_number().over(w_dup))

df_duplicates = df_silver.filter(F.col("_dup_rank") > 1)
df_silver     = df_silver.filter(F.col("_dup_rank") == 1).drop("_dup_rank")

dup_count = df_duplicates.count()
count_after = trace("4. after dedup", df_silver)
print(f"Exact duplicate rows moved to quarantine: {dup_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Data Quality Validation (Hard rules → Quarantine)
# MAGIC ทุก rule คืน **NULL เมื่อผ่าน** / ข้อความเมื่อไม่ผ่าน → รวมเป็น array → แถวที่ `_dq_reason IS NULL` = valid

# COMMAND ----------

REQUIRED_COLUMNS = ["is_high_defect", "production_volume", "defect_rate"]

PERCENT_COLUMNS = [            # 0–100
    "supplier_quality_pct", "quality_score_pct", "downtime_pct", "worker_productivity_pct",
]
RATIO_0_1_COLUMNS = [          # 0–1  (v2)
    "stockout_rate", "energy_efficiency_factor",
]
NON_NEGATIVE_COLUMNS = [
    "production_volume", "production_cost", "delivery_delay_days", "defect_rate",
    "maintenance_hours_per_week", "inventory_turnover_ratio", "safety_incidents_count",
    "energy_consumption_kwh", "additive_process_time_hrs", "additive_material_cost_usd",
]

dq_rules = []
for c in REQUIRED_COLUMNS:
    dq_rules.append((f"required:{c}", F.when(F.col(c).isNull(), F.lit(f"{c}: required value is NULL"))))

dq_rules.append(("target_domain",
    F.when(F.col("is_high_defect").isNotNull() & ~F.col("is_high_defect").isin(0, 1),
           F.lit("is_high_defect: value must be 0 or 1"))))

for c in PERCENT_COLUMNS:
    dq_rules.append((f"pct_range:{c}",
        F.when(F.col(c).isNotNull() & ((F.col(c) < 0) | (F.col(c) > 100)),
               F.lit(f"{c}: value must be between 0 and 100"))))

for c in RATIO_0_1_COLUMNS:
    dq_rules.append((f"ratio_range:{c}",
        F.when(F.col(c).isNotNull() & ((F.col(c) < 0) | (F.col(c) > 1)),
               F.lit(f"{c}: value must be between 0 and 1"))))

for c in NON_NEGATIVE_COLUMNS:
    dq_rules.append((f"non_negative:{c}",
        F.when(F.col(c).isNotNull() & (F.col(c) < 0), F.lit(f"{c}: value cannot be negative"))))

dq_rules.append(("max_hours_per_week",
    F.when(F.col("maintenance_hours_per_week") > 168,
           F.lit("maintenance_hours_per_week: cannot exceed 168 hours/week"))))

for c, t in EXPECTED_TYPES.items():
    if t == "double":
        dq_rules.append((f"nan:{c}", F.when(F.isnan(F.col(c)), F.lit(f"{c}: NaN is not allowed"))))

df_silver = df_silver.withColumn(
    "_dq_errors",
    F.concat(F.col("_dq_cast_errors"), F.array_compact(F.array(*[e for _, e in dq_rules]))),
).withColumn(
    "_dq_reason", F.when(F.size("_dq_errors") > 0, F.array_join("_dq_errors", "; "))
)

rule_fail_counts = df_silver.select(
    F.sum((F.size("_dq_cast_errors") > 0).cast("int")).alias("cast_error"),
    *[F.sum(e.isNotNull().cast("int")).alias(n.replace(":", "__")) for n, e in dq_rules],
)
print("Rows failing each DQ rule:")
display(rule_fail_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Business Rule Warnings (ไม่ตัดแถว — ติดธงไว้ให้ Gold/BI ใช้)

# COMMAND ----------

warning_expressions = [
    F.when((F.col("production_volume") == 0) & (F.col("production_cost") > 0),
           F.lit("production_volume=0 but production_cost>0")),
    F.when((F.col("production_volume") == 0) & (F.col("energy_consumption_kwh") > 0),
           F.lit("production_volume=0 but energy_consumption_kwh>0")),
    # v2: defect_rate สูงแต่ถูก label ว่า low defect (หรือกลับกัน) — label อาจไม่สอดคล้อง
    F.when((F.col("defect_rate") > 4.5) & (F.col("is_high_defect") == 0),
           F.lit("defect_rate>4.5 but is_high_defect=0")),
    F.when((F.col("quality_score_pct") < 60),
           F.lit("quality_score_pct below 60 (unusual)")),
]

df_silver = df_silver.withColumn(
    "_dq_warning_reason", F.array_join(F.array_compact(F.array(*warning_expressions)), "; ")
).withColumn(
    "_dq_warning_reason", F.when(F.length("_dq_warning_reason") > 0, F.col("_dq_warning_reason"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Split Valid / Quarantine
# MAGIC Quarantine = (DQ fail) ∪ (exact duplicates) ∪ (malformed rows จาก Bronze)

# COMMAND ----------

df_silver_valid  = df_silver.filter(F.col("_dq_reason").isNull())
df_dq_fail       = df_silver.filter(F.col("_dq_reason").isNotNull())

valid_count   = trace("7a. valid", df_silver_valid)
dq_fail_count = trace("7b. dq fail", df_dq_fail)
assert valid_count + dq_fail_count == count_after, "valid + dq_fail ต้องเท่ากับจำนวนหลัง dedup"

BRONZE_LINEAGE = ["source_file", "source_file_modified_at", "bronze_batch_id", "bronze_ingested_at"]
QUAR_COLS = BUSINESS_COLUMNS + ["record_id"] + BRONZE_LINEAGE

df_quarantine = (
    df_dq_fail.select(*QUAR_COLS, "_dq_reason", "_dq_warning_reason")
    .unionByName(
        df_duplicates.select(*QUAR_COLS)
        .withColumn("_dq_reason", F.lit("exact duplicate of another record"))
        .withColumn("_dq_warning_reason", F.lit(None).cast("string"))
    )
)
if corrupt_count > 0:
    df_quarantine = df_quarantine.unionByName(
        bronze_corrupt.select(
            *[F.lit(None).cast(EXPECTED_TYPES[c]).alias(c) for c in BUSINESS_COLUMNS],
            F.lit(None).cast("string").alias("record_id"),
            F.col("_source_file").alias("source_file"),
            F.col("_source_file_modified_at").alias("source_file_modified_at"),
            F.col("_bronze_batch_id").alias("bronze_batch_id"),
            F.col("_ingested_at").alias("bronze_ingested_at"),
            F.concat(F.lit("malformed CSV row: "), F.col("_corrupt_record")).alias("_dq_reason"),
            F.lit(None).cast("string").alias("_dq_warning_reason"),
        ),
        allowMissingColumns=True,
    )

invalid_count = trace("7c. quarantine total", df_quarantine)
assert valid_count + invalid_count == bronze_count, "valid + quarantine ต้องเท่ากับจำนวน Bronze (reconciliation)"

if invalid_count > 0:
    display(df_quarantine.groupBy("_dq_reason").count().orderBy(F.desc("count")))
else:
    print("No invalid records found.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Business Rule Warning Report

# COMMAND ----------

df_warn = df_silver_valid.filter(F.col("_dq_warning_reason").isNotNull())
warning_count = df_warn.count()
print(f"Rows with business-rule warnings: {warning_count}")
if warning_count > 0:
    display(df_warn.groupBy("_dq_warning_reason").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Lineage Metadata
# MAGIC ทุกแถวย้อนได้ถึง: Bronze table → ไฟล์ต้นทาง (+ เวลาแก้ไขไฟล์) → รอบโหลด Bronze → รอบ Silver

# COMMAND ----------

def add_lineage(df):
    return (df.withColumn("source_table", F.lit(SOURCE_TABLE))
              .withColumn("silver_processed_at", F.current_timestamp())
              .withColumn("silver_batch_id", F.lit(silver_batch_id)))

LINEAGE_COLUMNS = ["source_table"] + BRONZE_LINEAGE + ["silver_processed_at", "silver_batch_id"]

FINAL_COLUMNS = ["record_id"] + BUSINESS_COLUMNS + ["_dq_warning_reason"] + LINEAGE_COLUMNS
df_silver_final     = add_lineage(df_silver_valid).select(*FINAL_COLUMNS)
df_quarantine_final = add_lineage(df_quarantine).select(
    "record_id", *BUSINESS_COLUMNS, "_dq_reason", "_dq_warning_reason", *LINEAGE_COLUMNS)

df_silver_final.printSchema()
display(df_silver_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # 10. Silver Profiling (ใช้ประกอบบท Data Understanding)

# COMMAND ----------

display(df_silver_final.select(*BUSINESS_COLUMNS).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max"))

print("Target class balance (is_high_defect):")
display(
    df_silver_final.groupBy("is_high_defect").count()
    .withColumn("pct", F.round(F.col("count") / F.lit(valid_count) * 100, 2))
    .orderBy("is_high_defect")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 11. Final Data Quality Gate

# COMMAND ----------

print("Row trace summary:")
for step, n in ROW_TRACE:
    print(f"  {step:<38} {n:>8}")

if bronze_count > 0 and valid_count == 0:
    raise ValueError("Silver quality gate failed: 0 valid rows")

missing_final_cols = set(FINAL_COLUMNS) - set(df_silver_final.columns)
if missing_final_cols:
    raise ValueError(f"Final Silver is missing columns: {sorted(missing_final_cols)}")

bad_target = df_silver_final.filter(F.col("is_high_defect").isNull() | ~F.col("is_high_defect").isin(0, 1)).count()
if bad_target > 0:
    raise ValueError(f"{bad_target} NULL/invalid values in is_high_defect")

dup_keys = df_silver_final.groupBy("record_id").count().filter("count > 1").count()
null_keys = df_silver_final.filter(F.col("record_id").isNull()).count()
if dup_keys or null_keys:
    raise ValueError(f"record_id not unique/not null (dup={dup_keys}, null={null_keys})")

print("Silver Data Quality Gate: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC # 12. Write Quarantine & Silver Tables (+ constraints, comments, run log)

# COMMAND ----------

for t in {SILVER_TABLE, QUARANTINE_TABLE, DQ_RUN_LOG}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {t.rsplit('.', 1)[0]}")

(df_quarantine_final.write.format("delta").mode(WRITE_MODE)
    .option("overwriteSchema", "true").saveAsTable(QUARANTINE_TABLE))
print(f"Quarantine table written: {QUARANTINE_TABLE} ({invalid_count} rows)")

(df_silver_final.write.format("delta").mode(WRITE_MODE)
    .option("overwriteSchema", "true").saveAsTable(SILVER_TABLE))
print(f"Silver table written: {SILVER_TABLE} ({valid_count} rows)")

# Delta CHECK constraints — ป้องกันข้อมูลผิดถูกเขียนเข้ามาภายหลัง
CHECKS = {
    "chk_target_binary":   "is_high_defect IN (0, 1)",
    "chk_record_id":       "record_id IS NOT NULL",
    "chk_pct_quality":     "quality_score_pct BETWEEN 0 AND 100",
    "chk_pct_supplier":    "supplier_quality_pct BETWEEN 0 AND 100",
    "chk_pct_downtime":    "downtime_pct BETWEEN 0 AND 100",
    "chk_stockout_ratio":  "stockout_rate BETWEEN 0 AND 1",
    "chk_nonneg_volume":   "production_volume >= 0",
    "chk_nonneg_defect":   "defect_rate >= 0",
    "chk_maint_hours":     "maintenance_hours_per_week BETWEEN 0 AND 168",
}
for name, expr in CHECKS.items():
    spark.sql(f"ALTER TABLE {SILVER_TABLE} DROP CONSTRAINT IF EXISTS {name}")
    spark.sql(f"ALTER TABLE {SILVER_TABLE} ADD CONSTRAINT {name} CHECK ({expr})")

spark.sql(f"""COMMENT ON TABLE {SILVER_TABLE} IS
'Silver: cleaned, typed, validated manufacturing defect records. 1 row = 1 production batch record. Key: record_id (SHA-256 of business values). Source: {SOURCE_TABLE}'""")
print("Constraints & comment applied")

# COMMAND ----------

run_log = spark.createDataFrame(
    [(silver_batch_id, step, int(n), run_started_at) for step, n in ROW_TRACE],
    "silver_batch_id string, step string, row_count long, run_started_at timestamp",
)
run_log.write.format("delta").mode("append").saveAsTable(DQ_RUN_LOG)

print(f"Bronze rows     : {spark.table(SOURCE_TABLE).count()}")
print(f"Silver rows     : {spark.table(SILVER_TABLE).count()}")
print(f"Quarantine rows : {spark.table(QUARANTINE_TABLE).count()}")
display(spark.table(DQ_RUN_LOG).filter(F.col("silver_batch_id") == silver_batch_id))
dbutils.notebook.exit(f"PASS|bronze={bronze_count}|valid={valid_count}|quarantine={invalid_count}")