# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold_Predictive — RDBMS Gold Layer (Delta + Lakebase PostgreSQL)
# MAGIC
# MAGIC **Input:** `silver_predictive.manufacturing_defect_dataset_v2` (จาก `Silver_Predictive_v2`)
# MAGIC
# MAGIC ตามแผนรายงานบทที่ 6: **Split Silver 1 ตาราง → 6 ตารางตาม Business Domain พร้อม PK/FK** แล้วโหลดเข้า RDBMS (**Databricks Lakebase = PostgreSQL 17**)
# MAGIC และสร้าง analytic marts สำหรับ Dashboard / Business Insight (บทที่ 8)
# MAGIC
# MAGIC ```
# MAGIC                       silver_predictive.manufacturing_defect_dataset_v2
# MAGIC                                      │
# MAGIC         ┌──────────────┬──────────────┬─┴────────────┬──────────────┬──────────────┐
# MAGIC         ▼              ▼              ▼              ▼              ▼              ▼
# MAGIC    production     supply_chain     quality      maintenance     inventory     operations
# MAGIC  (PK production_id)  (FK production_id → production.production_id ในทุกตารางลูก, 1 : 1)
# MAGIC ```
# MAGIC
# MAGIC | Gold table | Domain | PK | FK | Columns |
# MAGIC |---|---|---|---|---|
# MAGIC | `production` | Production | `production_id` | – | record_id (UNIQUE), volume, cost, cost_per_unit, volume_band |
# MAGIC | `supply_chain` | Supply Chain | `supply_chain_id` | `production_id` | supplier_quality_pct, delivery_delay_days, bands |
# MAGIC | `quality` | Quality | `quality_id` | `production_id` | defect_rate, quality_score_pct, **is_high_defect**, bands |
# MAGIC | `maintenance` | Maintenance | `maintenance_id` | `production_id` | maintenance_hours_per_week, downtime_pct, bands |
# MAGIC | `inventory` | Inventory | `inventory_id` | `production_id` | inventory_turnover_ratio, stockout_rate |
# MAGIC | `operations` | Workforce / Safety / Energy / Additive | `operations_id` | `production_id` | productivity, safety, energy, additive |
# MAGIC
# MAGIC ### v2.1 — สิ่งที่เปลี่ยนในชั้น Insight
# MAGIC | # | เดิม | ปัญหา | ใหม่ |
# MAGIC |---|---|---|---|
# MAGIC | 1 | ใช้ `defect_rate` และ `quality_score` เป็น "ปัจจัย" ใน risk score | ทั้งสองตัวเป็น **ตัววัดผลด้านคุณภาพ (outcome)** ไม่ใช่สิ่งที่ฝ่ายผลิตปรับได้โดยตรง → อธิบาย defect ด้วย defect (วนในตัวเอง) | แยก `driver_role` = **Controllable** (ปรับได้) / **Outcome** (ตัววัดผล) · risk score ใช้เฉพาะ Controllable · Outcome ใช้เป็น **QC alert** |
# MAGIC | 2 | กำหนด threshold ด้วยตา (maint > 11, volume > 700, QS < 80) | ไม่ตรงจุดเปลี่ยนจริง (เช่น volume 701–800 = 80.8% เท่าค่าเฉลี่ย) | **Threshold scan** หา cut-off ที่ high-defect % ต่างกันมากที่สุด → เก็บใน `mart_threshold_scan` |
# MAGIC | 3 | – | ยังไม่ได้ถามว่า label `is_high_defect` นิยามจากอะไร | **Unknown Question:** เทียบ label กับกฎ threshold → `mart_label_rule` |

# COMMAND ----------

# MAGIC %pip install pg8000 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config

# COMMAND ----------

import uuid
from datetime import datetime, timezone
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window

CATALOG      = "predictive_manufacring_defect"
SILVER_TABLE = f"{CATALOG}.silver_predictive.manufacturing_defect_dataset_v2"
GOLD_SCHEMA  = f"{CATALOG}.gold_predictive"

# Lakebase (PostgreSQL)
LAKEBASE_PROJECT_ID = "manufacturing-defect-gold"
PG_DATABASE         = "databricks_postgres"
PG_SCHEMA           = "gold"
UC_PG_CATALOG       = "manufacturing_gold_pg"          # ลงทะเบียน Lakebase เป็น catalog ใน Unity Catalog

gold_batch_id  = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
print("Gold batch id:", gold_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver + Input Gate

# COMMAND ----------

silver = spark.table(SILVER_TABLE)
silver_count = silver.count()
print("Silver rows:", silver_count)

assert silver_count > 0, "Silver ว่าง — รัน Silver_Predictive_v2 ก่อน"
assert silver.filter(F.col("record_id").isNull()).count() == 0, "record_id มี NULL"
assert silver.select("record_id").distinct().count() == silver_count, "record_id ไม่ unique"
print("Gold input gate: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Surrogate Key `production_id` + Bands
# MAGIC - `production_id` = BIGINT เรียงตาม `record_id` (hash ที่ deterministic) → รันซ้ำกับข้อมูลชุดเดิมได้ id เดิมทุกครั้ง
# MAGIC - v2.1: ขอบ band ของ maintenance / volume / quality score ปรับให้ตรงจุดเปลี่ยนที่ threshold scan (ข้อ 6.0) พบ เพื่อให้กราฟเห็น "ขั้นบันได" ชัด

# COMMAND ----------

base = silver.withColumn("production_id", F.row_number().over(Window.orderBy("record_id")).cast("bigint"))

def band(col, edges, labels):
    """edges = upper bounds (inclusive) ตามลำดับ; คืน (label, order)"""
    expr_label, expr_order = None, None
    for i, (edge, lab) in enumerate(zip(edges, labels), start=1):
        cond = F.col(col) <= edge
        expr_label = F.when(cond, lab) if expr_label is None else expr_label.when(cond, lab)
        expr_order = F.when(cond, i) if expr_order is None else expr_order.when(cond, i)
    return expr_label.otherwise(labels[-1]), expr_order.otherwise(len(labels))

BANDS = {
    "volume_band":           ("production_volume",          [300, 500, 700, 800, 10**9], ["100-300", "301-500", "501-700", "701-800", "801+"]),
    "supplier_quality_band": ("supplier_quality_pct",       [85, 90, 95, 100],           ["80-85%", "85-90%", "90-95%", "95-100%"]),
    "delivery_delay_band":   ("delivery_delay_days",        [0, 1, 2, 3, 4, 10**6],      ["0 d", "1 d", "2 d", "3 d", "4 d", "5+ d"]),
    "defect_rate_band":      ("defect_rate",                [1, 2, 3, 4, 10**6],         ["0.5-1", "1-2", "2-3", "3-4", "4-5"]),
    "quality_band":          ("quality_score_pct",          [75, 80, 90, 100],           ["60-75", "75-80", "80-90", "90-100"]),
    "maintenance_band":      ("maintenance_hours_per_week", [5, 10, 16, 168],            ["0-5 h", "6-10 h", "11-16 h", "17-23 h"]),
    "downtime_band":         ("downtime_pct",               [1, 2, 3, 4, 100],           ["0-1%", "1-2%", "2-3%", "3-4%", "4-5%"]),
    "stockout_band":         ("stockout_rate",              [0.025, 0.05, 0.075, 1],     ["0-2.5%", "2.5-5%", "5-7.5%", "7.5-10%"]),
    "productivity_band":     ("worker_productivity_pct",    [85, 90, 95, 100],           ["80-85%", "85-90%", "90-95%", "95-100%"]),
    "energy_eff_band":       ("energy_efficiency_factor",   [0.2, 0.3, 0.4, 1],          ["0.1-0.2", "0.2-0.3", "0.3-0.4", "0.4-0.5"]),
}
for name, (col, edges, labels) in BANDS.items():
    lab, order = band(col, edges, labels)
    base = base.withColumn(name, lab).withColumn(name + "_order", order)

gold_meta = [F.lit(gold_batch_id).alias("gold_batch_id"), F.current_timestamp().alias("gold_processed_at")]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split Silver → 6 Domain DataFrames (PK / FK)

# COMMAND ----------

df_production = base.select(
    "production_id", "record_id",
    "production_volume", "production_cost",
    F.round(F.col("production_cost") / F.nullif(F.col("production_volume"), F.lit(0)), 4).alias("cost_per_unit"),
    "volume_band", "volume_band_order",
    "silver_batch_id", *gold_meta,
)

df_supply_chain = base.select(
    F.col("production_id").alias("supply_chain_id"), "production_id",
    "supplier_quality_pct", "delivery_delay_days",
    "supplier_quality_band", "supplier_quality_band_order",
    "delivery_delay_band", "delivery_delay_band_order", *gold_meta,
)

df_quality = base.select(
    F.col("production_id").alias("quality_id"), "production_id",
    "defect_rate", "quality_score_pct", "is_high_defect",
    "defect_rate_band", "defect_rate_band_order", "quality_band", "quality_band_order",
    F.col("_dq_warning_reason").alias("dq_warning_reason"), *gold_meta,
)

df_maintenance = base.select(
    F.col("production_id").alias("maintenance_id"), "production_id",
    "maintenance_hours_per_week", "downtime_pct",
    "maintenance_band", "maintenance_band_order", "downtime_band", "downtime_band_order", *gold_meta,
)

df_inventory = base.select(
    F.col("production_id").alias("inventory_id"), "production_id",
    "inventory_turnover_ratio", "stockout_rate",
    "stockout_band", "stockout_band_order", *gold_meta,
)

df_operations = base.select(
    F.col("production_id").alias("operations_id"), "production_id",
    "worker_productivity_pct", "safety_incidents_count",
    "energy_consumption_kwh", "energy_efficiency_factor",
    F.round(F.col("energy_consumption_kwh") / F.nullif(F.col("production_volume"), F.lit(0)), 4).alias("energy_kwh_per_unit"),
    "additive_process_time_hrs", "additive_material_cost_usd",
    "productivity_band", "productivity_band_order", "energy_eff_band", "energy_eff_band_order", *gold_meta,
)

# (table name, df, pk column)
GOLD_TABLES = [
    ("production",   df_production,   "production_id"),
    ("supply_chain", df_supply_chain, "supply_chain_id"),
    ("quality",      df_quality,      "quality_id"),
    ("maintenance",  df_maintenance,  "maintenance_id"),
    ("inventory",    df_inventory,    "inventory_id"),
    ("operations",   df_operations,   "operations_id"),
]
for t, df, pk in GOLD_TABLES:
    print(f"{t:<13} columns={len(df.columns):>2}  {df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. PK / FK Integrity Validation (Spark)
# MAGIC PK → NULL check → Duplicate check · FK → ต้องพบใน parent PK

# COMMAND ----------

parent_keys = df_production.select("production_id")
checks = []
for t, df, pk in GOLD_TABLES:
    rows = df.count()
    pk_null = df.filter(F.col(pk).isNull()).count()
    pk_dup = rows - df.select(pk).distinct().count()
    if t == "production":
        orphan = 0
    else:
        orphan = df.join(parent_keys, "production_id", "left_anti").count()
    status = "PASS" if (rows == silver_count and pk_null == 0 and pk_dup == 0 and orphan == 0) else "FAIL"
    checks.append((t, pk, rows, pk_null, pk_dup, orphan, status))

df_checks = spark.createDataFrame(checks, "table string, pk string, rows long, pk_null long, pk_duplicate long, fk_orphan long, status string")
display(df_checks)
if df_checks.filter("status != 'PASS'").count() > 0:
    raise ValueError("PK/FK validation failed")
print("PK/FK validation (Spark): PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load Gold → Delta (Unity Catalog) พร้อม PRIMARY KEY / FOREIGN KEY constraints

# COMMAND ----------

def fq(t): return f"{GOLD_SCHEMA}.{t}"

# ลบ FK เดิมก่อน overwrite (ไม่งั้นแทนที่ตาราง parent ไม่ได้)
for t, _, _ in GOLD_TABLES[1:]:
    if spark.catalog.tableExists(fq(t)):
        spark.sql(f"ALTER TABLE {fq(t)} DROP CONSTRAINT IF EXISTS fk_{t}_production")

for t, df, pk in GOLD_TABLES:
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq(t)))
    spark.sql(f"ALTER TABLE {fq(t)} ALTER COLUMN {pk} SET NOT NULL")
    spark.sql(f"ALTER TABLE {fq(t)} DROP CONSTRAINT IF EXISTS pk_{t}")
    spark.sql(f"ALTER TABLE {fq(t)} ADD CONSTRAINT pk_{t} PRIMARY KEY ({pk})")
    print(f"Delta written: {fq(t)}")

for t, _, _ in GOLD_TABLES[1:]:
    spark.sql(f"ALTER TABLE {fq(t)} ALTER COLUMN production_id SET NOT NULL")
    spark.sql(f"""ALTER TABLE {fq(t)} ADD CONSTRAINT fk_{t}_production
                  FOREIGN KEY (production_id) REFERENCES {fq('production')}(production_id)""")

COMMENTS = {
    "production":   "Gold RDBMS: production batch (parent). PK production_id, UNIQUE record_id",
    "supply_chain": "Gold RDBMS: supplier quality & delivery delay. FK production_id",
    "quality":      "Gold RDBMS: defect rate, quality score, target is_high_defect. FK production_id",
    "maintenance":  "Gold RDBMS: maintenance hours & downtime. FK production_id",
    "inventory":    "Gold RDBMS: inventory turnover & stockout rate. FK production_id",
    "operations":   "Gold RDBMS: workforce, safety, energy, additive manufacturing. FK production_id",
}
for t, c in COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {fq(t)} IS '{c}'")
print("UC PK/FK constraints & comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Analytic Marts (Business Insight layer)
# MAGIC
# MAGIC ### 6.0 แยกบทบาทของตัวแปร: ปัจจัยที่จัดการได้ (Controllable) vs ตัววัดผล (Outcome)
# MAGIC
# MAGIC | บทบาท | ตัวแปร | ใช้ทำอะไร |
# MAGIC |---|---|---|
# MAGIC | **Controllable** — สิ่งที่ทีมปรับได้ก่อน/ระหว่างผลิต | maintenance hours, production volume, supplier quality, delivery delay, downtime, stockout, worker productivity, energy efficiency | หาคันโยก → ทำ **risk score ก่อนปล่อย batch** |
# MAGIC | **Outcome** — ผลลัพธ์ด้านคุณภาพที่วัดได้หลังผลิต | defect rate, quality score | ใช้เป็น **QC alert / KPI เฝ้าระวัง** ไม่ใช่สาเหตุ (ถ้าใช้อธิบาย high-defect จะเป็นการอธิบาย defect ด้วย defect) |
# MAGIC
# MAGIC **Threshold scan:** แต่ละตัวแปร ลอง cut-off ทุกจุด (percentile 2–98 หรือทุกค่าจำนวนเต็ม) แล้วเลือกจุดที่ high-defect % ของฝั่งเสี่ยงกับฝั่งปลอดภัย **ต่างกันมากที่สุด** (ทั้งสองฝั่งต้องมี ≥ 5% ของ batch) จากนั้นปัดเป็นตัวเลขที่ใช้สื่อสารง่าย (`NICE_STEP`)
# MAGIC ตัวแปรที่ต่าง ≥ 15 pp ถือเป็น **key factor**

# COMMAND ----------

# (driver, column, domain, role, lever / owner, band column)
DRIVER_CATALOG = [
    ("Maintenance hours/week", "maintenance_hours_per_week", "Maintenance",  "Controllable", "ทีมซ่อมบำรุง: แผน PM / overhaul เครื่องที่ต้องซ่อมบ่อย", "maintenance_band"),
    ("Production volume",      "production_volume",          "Production",   "Controllable", "ฝ่ายวางแผนผลิต: ขนาด batch / QC sampling",              "volume_band"),
    ("Supplier quality",       "supplier_quality_pct",       "Supply Chain", "Controllable", "จัดซื้อ: คัดเลือก/ประเมิน supplier",                    "supplier_quality_band"),
    ("Delivery delay",         "delivery_delay_days",        "Supply Chain", "Controllable", "จัดซื้อ/โลจิสติกส์",                                   "delivery_delay_band"),
    ("Downtime",               "downtime_pct",               "Maintenance",  "Controllable", "ทีมซ่อมบำรุง",                                         "downtime_band"),
    ("Stockout rate",          "stockout_rate",              "Inventory",    "Controllable", "วางแผนสต็อก",                                          "stockout_band"),
    ("Worker productivity",    "worker_productivity_pct",    "Operations",   "Controllable", "หัวหน้าไลน์ / HR",                                     "productivity_band"),
    ("Energy efficiency",      "energy_efficiency_factor",   "Operations",   "Controllable", "วิศวกรรม / พลังงาน",                                   "energy_eff_band"),
    ("Defect rate",            "defect_rate",                "Quality",      "Outcome",      "ตัววัดผล QC — ใช้เฝ้าระวัง ไม่ใช่คันโยก",               "defect_rate_band"),
    ("Quality score",          "quality_score_pct",          "Quality",      "Outcome",      "ตัววัดผล QC — ใช้เฝ้าระวัง ไม่ใช่คันโยก",               "quality_band"),
]
NICE_STEP = {"production_volume": 50, "defect_rate": 0.5, "quality_score_pct": 5,
             "supplier_quality_pct": 1, "worker_productivity_pct": 1, "downtime_pct": 0.1,
             "stockout_rate": 0.005, "energy_efficiency_factor": 0.01}   # อื่น ๆ (จำนวนเต็ม) ใช้ค่าเดิม
KEY_FACTOR_MIN_GAP_PP = 15
MIN_SIDE_SHARE = 0.05

pdf = base.select("is_high_defect", *[c for _, c, *_ in DRIVER_CATALOG]).toPandas()
n_all = len(pdf)

def scan_threshold(s, y):
    """คืน (cut, gap) ที่ |high-defect% ฝั่ง > cut  −  ฝั่ง ≤ cut| มากที่สุด"""
    cands = sorted(s.unique()) if pd.api.types.is_integer_dtype(s) else sorted(set(s.quantile([i / 100 for i in range(2, 99)]).round(4)))
    best = None
    for c in cands:
        hi = s > c
        k = int(hi.sum())
        if k < MIN_SIDE_SHARE * n_all or n_all - k < MIN_SIDE_SHARE * n_all:
            continue
        gap = y[hi].mean() - y[~hi].mean()
        if best is None or abs(gap) > abs(best[1]):
            best = (float(c), float(gap))
    return best

def fmt(v): return str(int(v)) if float(v).is_integer() else str(v)

scan_rows = []
for driver, col, domain, role, lever, _ in DRIVER_CATALOG:
    s, y = pdf[col], pdf["is_high_defect"]
    raw_cut, gap = scan_threshold(s, y)
    step = NICE_STEP.get(col, 1)
    thr = round(round(raw_cut / step) * step, 4)
    if gap > 0:      # ค่าสูง = เสี่ยง
        op, risky = ">", s > thr
    else:            # ค่าต่ำ = เสี่ยง
        op, risky = "<", s < thr
    risky_pct, safe_pct = y[risky].mean() * 100, y[~risky].mean() * 100
    scan_rows.append((driver, col, domain, role, lever, float(raw_cut), float(thr), op, f"{op} {fmt(thr)}",
                      int(risky.sum()), round(float(risky_pct), 2), round(float(safe_pct), 2),
                      round(float(risky_pct - safe_pct), 2), bool(risky_pct - safe_pct >= KEY_FACTOR_MIN_GAP_PP)))

mart_scan = spark.createDataFrame(scan_rows,
    """driver string, column_name string, domain string, driver_role string, lever string,
       raw_cut double, threshold double, operator string, rule string, risky_batches long,
       risky_side_high_defect_pct double, safe_side_high_defect_pct double, gap_pp double, is_key_factor boolean""")
mart_scan.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_threshold_scan"))
display(spark.table(fq("mart_threshold_scan")).orderBy("driver_role", F.desc("gap_pp")))

KEY_CONTROLLABLE = [(r[1], r[7], r[6]) for r in scan_rows if r[3] == "Controllable" and r[13]]   # (column, op, threshold)
KEY_OUTCOME      = [(r[1], r[7], r[6]) for r in scan_rows if r[3] == "Outcome" and r[13]]
print("Key controllable factors:", KEY_CONTROLLABLE)
print("Key outcome (QC alert)   :", KEY_OUTCOME)
assert KEY_CONTROLLABLE, "ไม่พบ controllable key factor — ตรวจข้อมูล/threshold"

def flag_sql(col, op, thr): return f"{col} {op} {fmt(thr)}"
CTRL_SQL = [flag_sql(*k) for k in KEY_CONTROLLABLE]
QC_SQL   = [flag_sql(*k) for k in KEY_OUTCOME]
print("Risk score rules (controllable):", CTRL_SQL)
print("QC alert rules  (outcome)     :", QC_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 batch_360 — 1 แถว/batch + controllable risk score + QC alert + label rule

# COMMAND ----------

p, s, q, m, i, o = [spark.table(fq(t)).alias(a) for (t, _, _), a in zip(GOLD_TABLES, "psqmio")]

batch_360 = (
    p.join(s, "production_id").join(q, "production_id").join(m, "production_id")
     .join(i, "production_id").join(o, "production_id")
     .select(
        "production_id", "p.record_id",
        "p.production_volume", "p.production_cost", "p.cost_per_unit", "p.volume_band", "p.volume_band_order",
        "s.supplier_quality_pct", "s.delivery_delay_days", "s.supplier_quality_band", "s.supplier_quality_band_order",
        "s.delivery_delay_band", "s.delivery_delay_band_order",
        "q.defect_rate", "q.quality_score_pct", "q.is_high_defect", "q.defect_rate_band", "q.defect_rate_band_order",
        "q.quality_band", "q.quality_band_order",
        "m.maintenance_hours_per_week", "m.downtime_pct", "m.maintenance_band", "m.maintenance_band_order",
        "m.downtime_band", "m.downtime_band_order",
        "i.inventory_turnover_ratio", "i.stockout_rate", "i.stockout_band", "i.stockout_band_order",
        "o.worker_productivity_pct", "o.safety_incidents_count", "o.energy_consumption_kwh",
        "o.energy_efficiency_factor", "o.energy_kwh_per_unit", "o.additive_process_time_hrs",
        "o.additive_material_cost_usd", "o.productivity_band", "o.productivity_band_order",
        "o.energy_eff_band", "o.energy_eff_band_order",
     )
)
for col, op, thr in KEY_CONTROLLABLE + KEY_OUTCOME:
    batch_360 = batch_360.withColumn(f"flag_{col}", F.expr(flag_sql(col, op, thr)).cast("int"))

ctrl_count = sum(F.col(f"flag_{c}") for c, _, _ in KEY_CONTROLLABLE)
qc_any     = sum(F.col(f"flag_{c}") for c, _, _ in KEY_OUTCOME) if KEY_OUTCOME else F.lit(0)
batch_360 = (
    batch_360
    .withColumn("controllable_risk_count", ctrl_count)
    # คงคอลัมน์เดิมไว้ให้ notebook อื่น (เช่น ML_Predictive) ใช้ต่อได้: จำนวน flag ทั้งหมด (controllable + outcome) ตาม threshold ใหม่
    .withColumn("risk_flag_count", ctrl_count + sum(F.col(f"flag_{c}") for c, _, _ in KEY_OUTCOME))
    .withColumn("risk_segment", F.when(F.col("controllable_risk_count") == 0, "Low")
                                 .when(F.col("controllable_risk_count") == 1, "Medium").otherwise("High"))
    .withColumn("risk_segment_order", F.when(F.col("controllable_risk_count") == 0, 1)
                                       .when(F.col("controllable_risk_count") == 1, 2).otherwise(3))
    .withColumn("qc_alert", (qc_any > 0).cast("int"))
    # Unknown Question: label อธิบายได้ด้วยกฎ "เกิน threshold ข้อใดข้อหนึ่ง" หรือไม่
    .withColumn("label_rule_pred", ((ctrl_count + qc_any) > 0).cast("int"))
)
batch_360.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("batch_360"))
b360 = spark.table(fq("batch_360"))
print("batch_360 rows:", b360.count())

# COMMAND ----------

# 6.2 KPI overview (1 แถว)
kpi = b360.agg(
    F.count("*").alias("total_batches"),
    F.sum("production_volume").alias("total_units"),
    F.round(F.sum("production_cost"), 2).alias("total_production_cost"),
    F.round(F.sum("production_cost") / F.sum("production_volume"), 2).alias("avg_cost_per_unit"),
    F.sum("is_high_defect").alias("high_defect_batches"),
    F.round(F.avg("is_high_defect") * 100, 2).alias("high_defect_pct"),
    F.round(F.avg("defect_rate"), 3).alias("avg_defect_rate"),
    F.round(F.avg("quality_score_pct"), 2).alias("avg_quality_score"),
    F.round(F.avg("supplier_quality_pct"), 2).alias("avg_supplier_quality"),
    F.round(F.avg("maintenance_hours_per_week"), 2).alias("avg_maintenance_hours"),
    F.round(F.avg("downtime_pct"), 2).alias("avg_downtime_pct"),
    F.round(F.avg("stockout_rate") * 100, 2).alias("avg_stockout_pct"),
    F.round(F.avg("worker_productivity_pct"), 2).alias("avg_worker_productivity"),
    F.sum("safety_incidents_count").alias("total_safety_incidents"),
    F.round(F.sum("energy_consumption_kwh"), 0).alias("total_energy_kwh"),
    F.round(F.sum(F.when(F.col("is_high_defect") == 1, F.col("production_cost"))) / F.sum("production_cost") * 100, 2)
        .alias("cost_share_in_high_defect_pct"),
    F.round(F.avg((F.col("risk_segment") == "Low").cast("int")) * 100, 2).alias("low_risk_batch_pct"),
    F.round(F.avg("qc_alert") * 100, 2).alias("qc_alert_batch_pct"),
    F.round(F.avg((F.col("label_rule_pred") == F.col("is_high_defect")).cast("int")) * 100, 2).alias("label_rule_agreement_pct"),
)
kpi.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("kpi_overview"))
display(spark.table(fq("kpi_overview")))

# COMMAND ----------

# 6.3 High-defect % by driver band (long format) + driver_role
catalog_df = spark.createDataFrame([(d, dom, role, lever) for d, _, dom, role, lever, _ in DRIVER_CATALOG],
                                   "driver string, domain string, driver_role string, lever string")
parts = []
for driver, col, domain, role, lever, bcol in DRIVER_CATALOG:
    parts.append(
        b360.groupBy(F.col(bcol).alias("band"), F.col(bcol + "_order").alias("band_order"))
        .agg(F.count("*").alias("batches"),
             F.sum("is_high_defect").alias("high_defect_batches"),
             F.round(F.avg("is_high_defect") * 100, 2).alias("high_defect_pct"),
             F.round(F.avg("defect_rate"), 3).alias("avg_defect_rate"),
             F.round(F.sum("production_cost") / F.sum("production_volume"), 2).alias("avg_cost_per_unit"),
             F.sum("production_volume").alias("total_units"))
        .withColumn("driver", F.lit(driver)).withColumn("domain", F.lit(domain)).withColumn("driver_role", F.lit(role))
    )
from functools import reduce
mart_bands = reduce(lambda a, b: a.unionByName(b), parts).select(
    "driver", "domain", "driver_role", "band_order", "band", "batches", "high_defect_batches",
    "high_defect_pct", "avg_defect_rate", "avg_cost_per_unit", "total_units")
mart_bands.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_driver_bands"))
display(spark.table(fq("mart_driver_bands")).orderBy("driver_role", "driver", "band_order"))

# COMMAND ----------

# 6.4 Driver impact ranking — spread ระหว่าง band + correlation + threshold scan + role
spread = (spark.table(fq("mart_driver_bands")).groupBy("driver")
          .agg(F.round(F.max("high_defect_pct") - F.min("high_defect_pct"), 2).alias("high_defect_pct_spread"),
               F.min("high_defect_pct").alias("best_band_high_defect_pct"),
               F.max("high_defect_pct").alias("worst_band_high_defect_pct")))
corr_row = b360.select(*[F.corr(F.col(c).cast("double"), F.col("is_high_defect").cast("double")).alias(d)
                         for d, c, *_ in DRIVER_CATALOG]).first().asDict()
corr_df = spark.createDataFrame([(d, float(round(v, 4))) for d, v in corr_row.items()], "driver string, corr_with_high_defect double")
mart_impact = (catalog_df.join(spread, "driver").join(corr_df, "driver")
               .join(spark.table(fq("mart_threshold_scan")).select("driver", "threshold", "operator", "rule", "gap_pp", "is_key_factor"), "driver")
               .withColumn("impact_level", F.when(F.col("gap_pp") >= KEY_FACTOR_MIN_GAP_PP, "Strong")
                                            .when(F.col("gap_pp") >= 7, "Moderate").otherwise("Weak")))
mart_impact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_driver_impact"))
display(spark.table(fq("mart_driver_impact")).orderBy("driver_role", F.desc("gap_pp")))

# COMMAND ----------

# 6.5 Risk matrix: maintenance band × volume band (ทั้งคู่เป็น Controllable → บอกได้ว่าควรปรับตรงไหน)
mart_matrix = (b360.groupBy("maintenance_band", "maintenance_band_order", "volume_band", "volume_band_order")
               .agg(F.count("*").alias("batches"), F.round(F.avg("is_high_defect") * 100, 2).alias("high_defect_pct")))
mart_matrix.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_risk_matrix"))

# 6.6 Risk segment (controllable risk score)
total_cost = b360.agg(F.sum("production_cost")).first()[0]
mart_segment = (b360.groupBy("risk_segment", "risk_segment_order", "controllable_risk_count")
                .agg(F.count("*").alias("batches"),
                     F.round(F.avg("is_high_defect") * 100, 2).alias("high_defect_pct"),
                     F.round(F.sum("production_cost"), 2).alias("production_cost"),
                     F.sum("production_volume").alias("total_units"))
                .withColumn("batch_share_pct", F.round(F.col("batches") / F.lit(silver_count) * 100, 2))
                .withColumn("cost_share_pct", F.round(F.col("production_cost") / F.lit(total_cost) * 100, 2)))
mart_segment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_risk_segment"))
display(spark.table(fq("mart_risk_segment")).orderBy("controllable_risk_count"))

# 6.7 Action matrix: ก่อนผลิต (controllable segment) × หลังผลิต (QC alert)
mart_action = (b360.groupBy("risk_segment", "risk_segment_order", "qc_alert")
               .agg(F.count("*").alias("batches"), F.round(F.avg("is_high_defect") * 100, 2).alias("high_defect_pct"))
               .withColumn("qc_status", F.when(F.col("qc_alert") == 1, "QC alert").otherwise("QC pass"))
               .withColumn("action", F.when((F.col("risk_segment") == "Low") & (F.col("qc_alert") == 0), "ปล่อย batch ได้")
                                      .when(F.col("risk_segment") == "Low", "ตรวจซ้ำก่อนปล่อย (QC จับได้หลังผลิต)")
                                      .otherwise("แก้ที่กระบวนการก่อนผลิต (ซ่อมบำรุง / ขนาด batch)")))
mart_action.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_action_matrix"))
display(spark.table(fq("mart_action_matrix")).orderBy("risk_segment_order", "qc_alert"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.8 Unknown Question — "label `is_high_defect` นิยามจากอะไร?"
# MAGIC ข้อสังเกต: batch ที่ defect rate ต่ำสุด (0.5–1) ยังเป็น high-defect ถึง ~75% → label ไม่ได้มาจาก defect rate อย่างเดียว
# MAGIC ทดสอบสมมติฐาน: **label = 1 เมื่อเกิน threshold ข้อใดข้อหนึ่ง** (key factor ทั้ง controllable + outcome) แล้วดูว่าตรงกับ label จริงกี่ %

# COMMAND ----------

mart_label = (b360.groupBy("label_rule_pred", "is_high_defect").count()
              .withColumn("share_pct", F.round(F.col("count") / F.lit(silver_count) * 100, 2))
              .withColumn("outcome", F.when(F.col("label_rule_pred") == F.col("is_high_defect"), "rule agrees with label")
                                      .when(F.col("is_high_defect") == 1, "label=1 but no threshold exceeded")
                                      .otherwise("threshold exceeded but label=0")))
mart_label.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_label_rule"))
display(spark.table(fq("mart_label_rule")).orderBy("label_rule_pred", "is_high_defect"))

# high-defect % ต่อชั่วโมงซ่อมบำรุงทีละค่า → เห็นจุดกระโดดชัดเจน
mart_curve = (b360.groupBy("maintenance_hours_per_week")
              .agg(F.count("*").alias("batches"), F.round(F.avg("is_high_defect") * 100, 2).alias("high_defect_pct")))
mart_curve.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("mart_maint_hours_curve"))

agree = spark.table(fq("kpi_overview")).first()["label_rule_agreement_pct"]
print(f"กฎ threshold ({' OR '.join(CTRL_SQL + QC_SQL)}) ตรงกับ label {agree}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lakebase (PostgreSQL) — Connect
# MAGIC ใช้ Databricks SDK สร้าง/หา Lakebase project → endpoint → ขอ OAuth token อายุสั้น (ไม่มี password ฝังในโค้ด)
# MAGIC ใช้ driver `pg8000` (pure-Python) — `psycopg[binary]` ทำให้ Python kernel บน serverless ค้าง (ทดสอบแล้ว)

# COMMAND ----------

import ssl
import pg8000.native
from pg8000.exceptions import DatabaseError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import postgres as pgapi

w = WorkspaceClient()
PG_USER = w.current_user.me().user_name

try:
    project = w.postgres.get_project(name=f"projects/{LAKEBASE_PROJECT_ID}")
except NotFound:
    project = w.postgres.create_project(
        project=pgapi.Project(spec=pgapi.ProjectSpec(display_name="Manufacturing Defect Gold")),
        project_id=LAKEBASE_PROJECT_ID).wait()

branch_name = project.status.default_branch
endpoint = next(e for e in w.postgres.list_endpoints(parent=branch_name)
                if e.status.endpoint_type == pgapi.EndpointType.ENDPOINT_TYPE_READ_WRITE)
PG_HOST = endpoint.status.hosts.host
print("Project :", project.name, "| PG", project.status.pg_version)
print("Branch  :", branch_name)
print("Endpoint:", endpoint.name, endpoint.status.current_state)

# Enable endpoint if it was disabled
endpoint = w.postgres.update_endpoint(
    name=endpoint.name,
    endpoint=pgapi.Endpoint(name=endpoint.name, spec=pgapi.EndpointSpec(
        endpoint_type=pgapi.EndpointType.ENDPOINT_TYPE_READ_WRITE,
        disabled=False,
    )),
    update_mask=pgapi.FieldMask(field_mask=["spec.disabled"]),
).wait()
PG_HOST = endpoint.status.hosts.host
print("Endpoint enabled:", endpoint.name, endpoint.status.current_state)

def pg_connect():
    """เปิด connection ใหม่ด้วย OAuth token อายุสั้น (autocommit)"""
    token = w.postgres.generate_database_credential(endpoint=endpoint.name).token
    return pg8000.native.Connection(user=PG_USER, password=token, host=PG_HOST, database=PG_DATABASE,
                                    ssl_context=ssl.create_default_context(), timeout=120)

def run_script(conn, sql):
    """รัน SQL หลาย statement (คั่นด้วย ;)"""
    for stmt in [x.strip() for x in sql.split(";") if x.strip()]:
        conn.run(stmt)

conn = pg_connect()
print(conn.run("select version()")[0][0])
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. PostgreSQL DDL — 6 ตาราง พร้อม PK / FK / CHECK

# COMMAND ----------

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};

DROP VIEW IF EXISTS {PG_SCHEMA}.vw_label_rule_check, {PG_SCHEMA}.vw_risk_segment, {PG_SCHEMA}.vw_defect_by_maintenance,
                    {PG_SCHEMA}.vw_kpi_overview, {PG_SCHEMA}.vw_batch_360;

DROP TABLE IF EXISTS {PG_SCHEMA}.supply_chain, {PG_SCHEMA}.quality, {PG_SCHEMA}.maintenance,
                     {PG_SCHEMA}.inventory, {PG_SCHEMA}.operations, {PG_SCHEMA}.production CASCADE;

CREATE TABLE {PG_SCHEMA}.production (
    production_id      BIGINT        PRIMARY KEY,
    record_id          CHAR(64)      NOT NULL UNIQUE,
    production_volume  INTEGER       NOT NULL CHECK (production_volume >= 0),
    production_cost    NUMERIC(14,4) NOT NULL CHECK (production_cost >= 0),
    cost_per_unit      NUMERIC(14,4),
    volume_band        VARCHAR(20),
    volume_band_order  SMALLINT,
    silver_batch_id    VARCHAR(36),
    gold_batch_id      VARCHAR(36),
    gold_processed_at  TIMESTAMPTZ
);

CREATE TABLE {PG_SCHEMA}.supply_chain (
    supply_chain_id             BIGINT PRIMARY KEY,
    production_id               BIGINT NOT NULL UNIQUE REFERENCES {PG_SCHEMA}.production(production_id) ON DELETE CASCADE,
    supplier_quality_pct        NUMERIC(8,4) CHECK (supplier_quality_pct BETWEEN 0 AND 100),
    delivery_delay_days         INTEGER      CHECK (delivery_delay_days >= 0),
    supplier_quality_band       VARCHAR(20),
    supplier_quality_band_order SMALLINT,
    delivery_delay_band         VARCHAR(20),
    delivery_delay_band_order   SMALLINT,
    gold_batch_id               VARCHAR(36),
    gold_processed_at           TIMESTAMPTZ
);

CREATE TABLE {PG_SCHEMA}.quality (
    quality_id             BIGINT PRIMARY KEY,
    production_id          BIGINT NOT NULL UNIQUE REFERENCES {PG_SCHEMA}.production(production_id) ON DELETE CASCADE,
    defect_rate            NUMERIC(10,6) NOT NULL CHECK (defect_rate >= 0),
    quality_score_pct      NUMERIC(8,4)  CHECK (quality_score_pct BETWEEN 0 AND 100),
    is_high_defect         SMALLINT      NOT NULL CHECK (is_high_defect IN (0,1)),
    defect_rate_band       VARCHAR(20),
    defect_rate_band_order SMALLINT,
    quality_band           VARCHAR(20),
    quality_band_order     SMALLINT,
    dq_warning_reason      TEXT,
    gold_batch_id          VARCHAR(36),
    gold_processed_at      TIMESTAMPTZ
);

CREATE TABLE {PG_SCHEMA}.maintenance (
    maintenance_id             BIGINT PRIMARY KEY,
    production_id              BIGINT NOT NULL UNIQUE REFERENCES {PG_SCHEMA}.production(production_id) ON DELETE CASCADE,
    maintenance_hours_per_week INTEGER      CHECK (maintenance_hours_per_week BETWEEN 0 AND 168),
    downtime_pct               NUMERIC(8,4) CHECK (downtime_pct BETWEEN 0 AND 100),
    maintenance_band           VARCHAR(20),
    maintenance_band_order     SMALLINT,
    downtime_band              VARCHAR(20),
    downtime_band_order        SMALLINT,
    gold_batch_id              VARCHAR(36),
    gold_processed_at          TIMESTAMPTZ
);

CREATE TABLE {PG_SCHEMA}.inventory (
    inventory_id             BIGINT PRIMARY KEY,
    production_id            BIGINT NOT NULL UNIQUE REFERENCES {PG_SCHEMA}.production(production_id) ON DELETE CASCADE,
    inventory_turnover_ratio NUMERIC(10,4) CHECK (inventory_turnover_ratio >= 0),
    stockout_rate            NUMERIC(8,6)  CHECK (stockout_rate BETWEEN 0 AND 1),
    stockout_band            VARCHAR(20),
    stockout_band_order      SMALLINT,
    gold_batch_id            VARCHAR(36),
    gold_processed_at        TIMESTAMPTZ
);

CREATE TABLE {PG_SCHEMA}.operations (
    operations_id              BIGINT PRIMARY KEY,
    production_id              BIGINT NOT NULL UNIQUE REFERENCES {PG_SCHEMA}.production(production_id) ON DELETE CASCADE,
    worker_productivity_pct    NUMERIC(8,4)  CHECK (worker_productivity_pct BETWEEN 0 AND 100),
    safety_incidents_count     INTEGER       CHECK (safety_incidents_count >= 0),
    energy_consumption_kwh     NUMERIC(14,4) CHECK (energy_consumption_kwh >= 0),
    energy_efficiency_factor   NUMERIC(8,6)  CHECK (energy_efficiency_factor BETWEEN 0 AND 1),
    energy_kwh_per_unit        NUMERIC(14,4),
    additive_process_time_hrs  NUMERIC(10,4) CHECK (additive_process_time_hrs >= 0),
    additive_material_cost_usd NUMERIC(12,4) CHECK (additive_material_cost_usd >= 0),
    productivity_band          VARCHAR(20),
    productivity_band_order    SMALLINT,
    energy_eff_band            VARCHAR(20),
    energy_eff_band_order      SMALLINT,
    gold_batch_id              VARCHAR(36),
    gold_processed_at          TIMESTAMPTZ
);

COMMENT ON TABLE {PG_SCHEMA}.production   IS 'Parent table: 1 row = 1 production batch record';
COMMENT ON TABLE {PG_SCHEMA}.supply_chain IS 'Supplier quality and delivery delay (1:1 production)';
COMMENT ON TABLE {PG_SCHEMA}.quality      IS 'Defect rate, quality score and target is_high_defect (1:1 production)';
COMMENT ON TABLE {PG_SCHEMA}.maintenance  IS 'Maintenance hours and downtime (1:1 production)';
COMMENT ON TABLE {PG_SCHEMA}.inventory    IS 'Inventory turnover and stockout rate (1:1 production)';
COMMENT ON TABLE {PG_SCHEMA}.operations   IS 'Workforce, safety, energy and additive manufacturing (1:1 production)';
"""

conn = pg_connect()
try:
    conn.run("BEGIN")
    run_script(conn, DDL)
    conn.run("COMMIT")
finally:
    conn.close()
print("PostgreSQL DDL applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Load Gold → PostgreSQL (COPY, parent ก่อน child)

# COMMAND ----------

import io, csv

def pg_columns(conn, table):
    rows = conn.run("""select column_name from information_schema.columns
                       where table_schema = :s and table_name = :t order by ordinal_position""",
                    s=PG_SCHEMA, t=table)
    return [r[0] for r in rows]

load_counts = {}
conn = pg_connect()
try:
    conn.run("BEGIN")                                   # โหลดทั้ง 6 ตารางใน transaction เดียว
    for t, _, _ in GOLD_TABLES:                         # production อยู่ลำดับแรก → FK ผ่าน
        cols = pg_columns(conn, t)
        rows = spark.table(fq(t)).select(*cols).collect()
        buf = io.StringIO()
        writer = csv.writer(buf)
        for r in rows:
            writer.writerow(["" if v is None else v for v in r])   # ช่องว่าง (ไม่ quote) = NULL ใน CSV COPY
        buf.seek(0)
        conn.run(f"COPY {PG_SCHEMA}.{t} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)", stream=buf)
        load_counts[t] = len(rows)
        print(f"Loaded {t:<13} {len(rows):>6} rows")
    conn.run("COMMIT")
except Exception:
    conn.run("ROLLBACK")
    raise
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. PostgreSQL Views สำหรับ BI (SQL ที่ใช้ตอบ Business Questions)
# MAGIC เงื่อนไข risk score / QC alert ใน view สร้างจาก threshold ชุดเดียวกับข้อ 6.0 → Delta และ PostgreSQL ให้ผลตรงกัน

# COMMAND ----------

ctrl_count_sql = " + ".join(f"({x})::int" for x in CTRL_SQL)
qc_any_sql     = " OR ".join(f"({x})" for x in QC_SQL) if QC_SQL else "FALSE"
all_count_sql  = " + ".join(f"({x})::int" for x in CTRL_SQL + QC_SQL)

VIEWS = f"""
CREATE OR REPLACE VIEW {PG_SCHEMA}.vw_batch_360 AS
SELECT p.production_id, p.production_volume, p.production_cost, p.cost_per_unit, p.volume_band,
       s.supplier_quality_pct, s.delivery_delay_days,
       q.defect_rate, q.quality_score_pct, q.is_high_defect, q.defect_rate_band, q.quality_band,
       m.maintenance_hours_per_week, m.downtime_pct, m.maintenance_band,
       i.inventory_turnover_ratio, i.stockout_rate,
       o.worker_productivity_pct, o.safety_incidents_count, o.energy_consumption_kwh, o.energy_efficiency_factor,
       ({ctrl_count_sql})                      AS controllable_risk_count,
       ({all_count_sql})                       AS risk_flag_count,
       ({qc_any_sql})::int                     AS qc_alert,
       (({ctrl_count_sql}) > 0 OR {qc_any_sql})::int AS label_rule_pred
FROM {PG_SCHEMA}.production p
JOIN {PG_SCHEMA}.supply_chain s USING (production_id)
JOIN {PG_SCHEMA}.quality      q USING (production_id)
JOIN {PG_SCHEMA}.maintenance  m USING (production_id)
JOIN {PG_SCHEMA}.inventory    i USING (production_id)
JOIN {PG_SCHEMA}.operations   o USING (production_id);

CREATE OR REPLACE VIEW {PG_SCHEMA}.vw_kpi_overview AS
SELECT COUNT(*)                                                AS total_batches,
       SUM(production_volume)                                  AS total_units,
       ROUND(SUM(production_cost), 2)                          AS total_production_cost,
       ROUND(SUM(production_cost) / SUM(production_volume), 2) AS avg_cost_per_unit,
       ROUND(AVG(is_high_defect) * 100, 2)                     AS high_defect_pct,
       ROUND(AVG(defect_rate), 3)                              AS avg_defect_rate,
       ROUND(AVG(quality_score_pct), 2)                        AS avg_quality_score,
       ROUND(AVG(maintenance_hours_per_week), 2)               AS avg_maintenance_hours,
       ROUND(AVG(downtime_pct), 2)                             AS avg_downtime_pct
FROM {PG_SCHEMA}.vw_batch_360;

CREATE OR REPLACE VIEW {PG_SCHEMA}.vw_defect_by_maintenance AS
SELECT m.maintenance_band, m.maintenance_band_order, COUNT(*) AS batches,
       ROUND(AVG(q.is_high_defect) * 100, 2) AS high_defect_pct
FROM {PG_SCHEMA}.maintenance m JOIN {PG_SCHEMA}.quality q USING (production_id)
GROUP BY 1, 2 ORDER BY 2;

CREATE OR REPLACE VIEW {PG_SCHEMA}.vw_risk_segment AS
SELECT CASE WHEN controllable_risk_count = 0 THEN 'Low' WHEN controllable_risk_count = 1 THEN 'Medium' ELSE 'High' END AS risk_segment,
       COUNT(*) AS batches,
       ROUND(AVG(is_high_defect) * 100, 2) AS high_defect_pct,
       ROUND(SUM(production_cost), 2) AS production_cost
FROM {PG_SCHEMA}.vw_batch_360
GROUP BY 1;

CREATE OR REPLACE VIEW {PG_SCHEMA}.vw_label_rule_check AS
SELECT label_rule_pred, is_high_defect, COUNT(*) AS batches,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS share_pct
FROM {PG_SCHEMA}.vw_batch_360
GROUP BY 1, 2;
"""
conn = pg_connect()
try:
    run_script(conn, VIEWS)
    print("Views created")
    for v in ["vw_kpi_overview", "vw_defect_by_maintenance", "vw_risk_segment", "vw_label_rule_check"]:
        rows = conn.run(f"select * from {PG_SCHEMA}.{v}")
        print(f"\n== {v}")
        print([c["name"] for c in conn.columns])
        for r in rows:
            print(r)
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. PK / FK Integrity Test ใน PostgreSQL (บทที่ 9)
# MAGIC 1. ตรวจ constraint ที่มีจริงใน catalog · 2. orphan check ด้วย LEFT JOIN · 3. row count Delta = PostgreSQL · 4. **Negative tests**: ใส่ FK ที่ไม่มีจริง / PK ซ้ำ / target ผิด → ต้องถูก DB ปฏิเสธ

# COMMAND ----------

def expect_rejected(name, sql, sqlstates):
    """Negative test: DB ต้องปฏิเสธ statement นี้ด้วย SQLSTATE ที่คาดไว้"""
    c = pg_connect()
    try:
        c.run(sql)
        return (name, "rejected " + "/".join(sqlstates), "accepted", "FAIL")
    except DatabaseError as e:
        code = e.args[0].get("C") if e.args and isinstance(e.args[0], dict) else str(e)[:40]
        return (name, "rejected " + "/".join(sqlstates), f"SQLSTATE {code}", "PASS" if code in sqlstates else "FAIL")
    finally:
        c.close()

pg_tests = []
conn = pg_connect()
try:
    cons = conn.run("""
        SELECT tc.table_name, tc.constraint_type, tc.constraint_name
        FROM information_schema.table_constraints tc
        WHERE tc.table_schema = :s AND tc.constraint_type IN ('PRIMARY KEY','FOREIGN KEY','UNIQUE')
        ORDER BY 1, 2""", s=PG_SCHEMA)
    print("Constraints in PostgreSQL:")
    for c in cons:
        print("  ", tuple(c))

    for t, _, _ in GOLD_TABLES[1:]:
        orphan = conn.run(f"""SELECT COUNT(*) FROM {PG_SCHEMA}.{t} c
                              LEFT JOIN {PG_SCHEMA}.production p USING (production_id)
                              WHERE p.production_id IS NULL""")[0][0]
        pg_tests.append((f"orphan check {t}", "0 orphan", str(orphan), "PASS" if orphan == 0 else "FAIL"))

    # Reconciliation Delta vs PostgreSQL
    for t, _, _ in GOLD_TABLES:
        n_pg = conn.run(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{t}")[0][0]
        n_dl = spark.table(fq(t)).count()
        pg_tests.append((f"row count {t}", str(n_dl), str(n_pg), "PASS" if n_pg == n_dl == silver_count else "FAIL"))
finally:
    conn.close()

# Negative tests (23503 = foreign_key_violation, 23505 = unique_violation, 23514 = check_violation)
pg_tests.append(expect_rejected("insert orphan FK (production_id=-999)",
    f"INSERT INTO {PG_SCHEMA}.quality (quality_id, production_id, defect_rate, is_high_defect) VALUES (-1, -999, 1.0, 1)", ["23503"]))
pg_tests.append(expect_rejected("insert duplicate PK (production_id=1)",
    f"INSERT INTO {PG_SCHEMA}.production (production_id, record_id, production_volume, production_cost) VALUES (1, repeat('x',64), 1, 1)", ["23505"]))
pg_tests.append(expect_rejected("insert invalid target is_high_defect=5",
    f"INSERT INTO {PG_SCHEMA}.quality (quality_id, production_id, defect_rate, is_high_defect) VALUES (-2, 1, 1.0, 5)", ["23514", "23505"]))

df_pg_tests = spark.createDataFrame(pg_tests, "test string, expected string, actual string, status string")
display(df_pg_tests)
if df_pg_tests.filter("status != 'PASS'").count():
    raise ValueError("PostgreSQL integrity tests failed")
print("PostgreSQL PK/FK integrity: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. ลงทะเบียน Lakebase เป็น Catalog ใน Unity Catalog
# MAGIC ทำให้ query ตาราง PostgreSQL ได้จาก SQL Editor / AI/BI Dashboard ด้วยชื่อ `manufacturing_gold_pg.gold.<table>`

# COMMAND ----------

try:
    w.postgres.get_catalog(name=f"catalogs/{UC_PG_CATALOG}")
    print("UC catalog already registered:", UC_PG_CATALOG)
except Exception:
    try:
        w.postgres.create_catalog(
            catalog=pgapi.Catalog(spec=pgapi.CatalogCatalogSpec(postgres_database=PG_DATABASE, branch=branch_name)),
            catalog_id=UC_PG_CATALOG).wait()
        print("Registered UC catalog:", UC_PG_CATALOG)
    except Exception as e:
        print("WARNING: register catalog failed (dashboard จะใช้ Delta gold แทน):", e)

try:
    display(spark.sql(f"SELECT * FROM {UC_PG_CATALOG}.{PG_SCHEMA}.vw_kpi_overview"))
except Exception as e:
    print("Query via UC catalog not available yet:", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Gold Run Log

# COMMAND ----------

log = spark.createDataFrame(
    [(gold_batch_id, t, int(load_counts.get(t, 0)), run_started_at, datetime.now(timezone.utc)) for t, _, _ in GOLD_TABLES],
    "gold_batch_id string, table_name string, pg_rows long, started_at timestamp, finished_at timestamp")
log.write.format("delta").mode("append").saveAsTable(fq("gold_run_log"))

print("Gold tables (Delta):")
display(spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}"))
print("Gold layer: PASS")