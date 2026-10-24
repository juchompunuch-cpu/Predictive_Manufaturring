# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # DQ_Test_Dirty_Data — พิสูจน์ว่ากลไก Data Quality ทำงานจริง
# MAGIC
# MAGIC **ปัญหา:** ข้อมูลต้นทางสะอาด (0 placeholder, 0 แถวซ้ำ, 0 แถวเสีย) → ใน production run ไม่มีแถวไหนเข้า quarantine จึงยังพิสูจน์ไม่ได้ว่า rule ทำงานจริง
# MAGIC
# MAGIC **วิธีทดสอบ:**
# MAGIC 1. สร้างไฟล์ทดสอบ = แถวจริง 200 แถว (สะอาด) + **แถวสกปรกที่ฉีดเข้าไปเอง 15 แถว** ครอบคลุมทุก rule
# MAGIC 2. รัน **notebook จริง** `Bronze_Predictive_v2` และ `Silver_Predictive_v2` (โค้ดชุดเดียวกับ production) โดยส่ง parameter ให้เขียนลง schema `dq_test` → **ไม่ทับตารางจริง**
# MAGIC 3. ตรวจผลทีละ test case: คาดว่าเกิดอะไร → เกิดจริงอะไร → PASS/FAIL แล้วบันทึกลง `dq_test.test_results`
# MAGIC
# MAGIC | กลุ่ม | สิ่งที่ทดสอบ | ผลที่คาด |
# MAGIC |---|---|---|
# MAGIC | Bronze | แถว format เสีย (คอลัมน์เกิน / ขาด) | เก็บใน `_corrupt_record` → Silver ส่ง quarantine |
# MAGIC | Silver – Missing | placeholder `N/A`, ค่าว่าง ในคอลัมน์บังคับ | → NULL → quarantine (required) |
# MAGIC | Silver – Missing | placeholder ในคอลัมน์ไม่บังคับ | → NULL แต่ **ยังเป็น valid** (ไม่ตัดแถวเกินจำเป็น) |
# MAGIC | Silver – Type | ข้อความในช่องตัวเลข, ทศนิยมในช่อง INT | quarantine (cast error) |
# MAGIC | Silver – Range | %, สัดส่วน 0–1, ค่าติดลบ, ชั่วโมง > 168, target ไม่ใช่ 0/1 | quarantine |
# MAGIC | Silver – Dedup | แถวซ้ำทุกคอลัมน์ | เก็บ 1 แถว อีกแถว quarantine |
# MAGIC | Silver – Cleaning | ช่องว่างหน้า/หลังตัวเลข | trim แล้ว valid |
# MAGIC | Silver – Warning | defect_rate > 4.5 แต่ label = 0 | valid + ติดธง warning |

# COMMAND ----------

import csv, io, os
import pyspark.sql.functions as F

CATALOG     = "predictive_manufacring_defect"
TEST_SCHEMA = f"{CATALOG}.dq_test"
SOURCE_FILE = "/Volumes/predictive_manufacring_defect/bronze_preditive/bronze_predic/manufacturing_defect_dataset.csv"
TEST_DIR    = "/Volumes/predictive_manufacring_defect/bronze_preditive/bronze_predic/dq_test"
TEST_FILE   = f"{TEST_DIR}/manufacturing_defect_dirty.csv"
N_CLEAN     = 200

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. สร้างไฟล์ข้อมูลสกปรก
# MAGIC แต่ละ test case ใช้แถวจริงคนละแถว (แถวที่ 201 เป็นต้นไป) แล้วแก้ค่า **1 จุด** ให้ผิดตามที่ต้องการทดสอบ

# COMMAND ----------

with open(SOURCE_FILE, encoding="utf-8-sig") as f:
    rows = list(csv.reader(f))
header, data = rows[0], rows[1:]
col = {c: i for i, c in enumerate(header)}

clean_rows = [r[:] for r in data[:N_CLEAN]]
pool = iter(data[N_CLEAN:])            # แถวจริงที่ยังไม่ถูกใช้ สำหรับทำ test case

def mutate(changes):
    r = next(pool)[:]
    for c, v in changes.items():
        r[col[c]] = v
    return r

# (case_id, คำอธิบาย, แถวที่จะเขียน, ผลที่คาด, ข้อความที่ต้องเจอ)
# expected: quarantine = ต้องอยู่ใน quarantine และ _dq_reason มีข้อความนี้
#           valid      = ต้องอยู่ใน Silver
#           warning    = ต้องอยู่ใน Silver และ _dq_warning_reason มีข้อความนี้
#           corrupt    = Bronze ต้องเก็บไว้ใน _corrupt_record
CASES = [
    ("T01", "placeholder 'N/A' ใน defect_rate (คอลัมน์บังคับ)",   mutate({"DefectRate": "N/A"}),          "quarantine", "defect_rate: required value is NULL"),
    ("T02", "ค่าว่างใน DefectStatus (target)",                     mutate({"DefectStatus": ""}),            "quarantine", "is_high_defect: required value is NULL"),
    ("T03", "ข้อความ 'abc' ใน ProductionCost",                     mutate({"ProductionCost": "abc"}),       "quarantine", "production_cost: invalid value for double"),
    ("T04", "ทศนิยม 12.5 ใน MaintenanceHours (INT)",               mutate({"MaintenanceHours": "12.5"}),    "quarantine", "maintenance_hours_per_week: non-integer value for int"),
    ("T05", "DefectStatus = 2 (ต้องเป็น 0/1)",                     mutate({"DefectStatus": "2"}),           "quarantine", "is_high_defect: value must be 0 or 1"),
    ("T06", "QualityScore = 150 (เกิน 100%)",                      mutate({"QualityScore": "150"}),         "quarantine", "quality_score_pct: value must be between 0 and 100"),
    ("T07", "StockoutRate = 5 (สัดส่วนต้องอยู่ 0–1)",               mutate({"StockoutRate": "5"}),           "quarantine", "stockout_rate: value must be between 0 and 1"),
    ("T08", "ProductionVolume = -10 (ติดลบ)",                      mutate({"ProductionVolume": "-10"}),     "quarantine", "production_volume: value cannot be negative"),
    ("T09", "MaintenanceHours = 200 (เกิน 168 ชม./สัปดาห์)",        mutate({"MaintenanceHours": "200"}),     "quarantine", "maintenance_hours_per_week: cannot exceed 168"),
    ("T10", "แถวซ้ำกับแถวสะอาดแถวแรกทุกคอลัมน์",                   clean_rows[0][:],                        "quarantine", "exact duplicate of another record"),
    ("T11", "placeholder 'N/A' ใน EnergyEfficiency (ไม่บังคับ)",    mutate({"EnergyEfficiency": "N/A"}),     "valid",      None),
    ("T12", "ช่องว่างหน้า/หลังตัวเลข ' 512 '",                      mutate({"ProductionVolume": " 512 "}),   "valid",      None),
    ("T13", "defect_rate 4.8 แต่ label = 0",                        mutate({"DefectRate": "4.8", "DefectStatus": "0"}), "warning", "defect_rate>4.5 but is_high_defect=0"),
]
# แถวที่ format เสีย (จำนวนคอลัมน์ไม่ตรง) — เขียนเป็นบรรทัดดิบ
row_extra = next(pool) + ["EXTRA_VALUE"]
row_short = next(pool)[:-3]
CORRUPT_CASES = [
    ("T14", "แถวมีคอลัมน์เกิน (18 ค่า)", row_extra, "corrupt", "malformed CSV row"),
    ("T15", "แถวมีคอลัมน์ขาด (14 ค่า)",  row_short, "corrupt", "malformed CSV row"),
]

buf = io.StringIO()
w = csv.writer(buf, lineterminator="\n")
w.writerow(header)
w.writerows(clean_rows)
w.writerows([c[2] for c in CASES])
w.writerows([c[2] for c in CORRUPT_CASES])

dbutils.fs.mkdirs(TEST_DIR)
dbutils.fs.put(TEST_FILE, buf.getvalue(), overwrite=True)

ALL_CASES = CASES + CORRUPT_CASES
n_lines = N_CLEAN + len(ALL_CASES)
print(f"Test file: {TEST_FILE}")
print(f"Data lines = {N_CLEAN} clean + {len(ALL_CASES)} test cases = {n_lines}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. รัน Bronze และ Silver ตัวจริง ด้วย parameter ของชุดทดสอบ

# COMMAND ----------

PARAMS_BRONZE = {
    "source_file":  TEST_FILE,
    "bronze_table": f"{TEST_SCHEMA}.bronze_dirty",
    "ingest_log":   f"{TEST_SCHEMA}.ingestion_log",
}
PARAMS_SILVER = {
    "source_table":     f"{TEST_SCHEMA}.bronze_dirty",
    "silver_table":     f"{TEST_SCHEMA}.silver_dirty",
    "quarantine_table": f"{TEST_SCHEMA}.quarantine_dirty",
    "dq_run_log":       f"{TEST_SCHEMA}.dq_run_log",
}
NOTEBOOK_DIR = "/Users/69056012@kmitl.ac.th/Predictive_Manufaturring/Predictive_Manu"
TIMEOUT = 1800  # serverless sub-notebook cold start can take >15 min

print("Bronze →", dbutils.notebook.run(f"{NOTEBOOK_DIR}/Bronze_Predictive_v2", TIMEOUT, PARAMS_BRONZE))
print("Silver →", dbutils.notebook.run(f"{NOTEBOOK_DIR}/Silver_Predictive_v2", TIMEOUT, PARAMS_SILVER))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ตรวจผลทีละ test case

# COMMAND ----------

bronze = spark.table(PARAMS_BRONZE["bronze_table"])
silver = spark.table(PARAMS_SILVER["silver_table"])
quar   = spark.table(PARAMS_SILVER["quarantine_table"])

quar_reasons   = [r["_dq_reason"] for r in quar.select("_dq_reason").collect()]
warn_reasons   = [r["_dq_warning_reason"] for r in silver.select("_dq_warning_reason").collect() if r["_dq_warning_reason"]]
corrupt_bronze = bronze.filter(F.col("_corrupt_record").isNotNull()).count()

results = []
def add(case_id, desc, expected, actual, ok):
    results.append((case_id, desc, expected, actual, "PASS" if ok else "FAIL"))

for case_id, desc, _, expected, text in CASES:
    if expected == "quarantine":
        hit = sum(text in r for r in quar_reasons)
        add(case_id, desc, f"quarantine: {text}", f"พบใน quarantine {hit} แถว", hit == 1)

# T11: placeholder ในคอลัมน์ไม่บังคับ → NULL แต่ valid
n_null_eff = silver.filter(F.col("energy_efficiency_factor").isNull()).count()
add("T11", CASES[10][1], "valid, energy_efficiency_factor = NULL", f"Silver มี NULL {n_null_eff} แถว", n_null_eff == 1)

# T12: trim แล้วต้องได้ตัวเลข 512 และอยู่ใน Silver
n_512 = silver.filter(F.col("production_volume") == 512).count()
other_valid_rows = clean_rows + [CASES[10][2], CASES[12][2]]          # แถว valid อื่นที่อาจมี 512 อยู่แล้ว
n_512_other = sum(1 for r in other_valid_rows if r[col["ProductionVolume"]].strip() == "512")
add("T12", CASES[11][1], "valid, production_volume = 512", f"Silver มี 512 จาก test {n_512 - n_512_other} แถว", n_512 - n_512_other == 1)

# T13: warning
hit_warn = sum(CASES[12][4] in r for r in warn_reasons)
add("T13", CASES[12][1], "valid + warning", f"warning {hit_warn} แถว", hit_warn == 1)

# T14–T15: corrupt
hit_corrupt = sum("malformed CSV row" in r for r in quar_reasons)
add("T14–T15", "แถว format เสีย 2 แถว", "Bronze corrupt = 2, quarantine = 2",
    f"Bronze corrupt = {corrupt_bronze}, quarantine = {hit_corrupt}", corrupt_bronze == 2 and hit_corrupt == 2)

# ภาพรวม (reconciliation)
exp_valid = N_CLEAN + 3                       # 200 แถวสะอาด + T11, T12, T13
exp_quar  = len(ALL_CASES) - 3                # ที่เหลือทั้งหมดต้องเข้า quarantine
n_b, n_s, n_q = bronze.count(), silver.count(), quar.count()
add("R1", "Bronze rows = บรรทัดในไฟล์", str(n_lines), str(n_b), n_b == n_lines)
add("R2", "Silver valid rows", str(exp_valid), str(n_s), n_s == exp_valid)
add("R3", "Quarantine rows", str(exp_quar), str(n_q), n_q == exp_quar)
add("R4", "valid + quarantine = Bronze", str(n_b), str(n_s + n_q), n_s + n_q == n_b)

# lineage ต้องครบทุกแถว (รวม source_file_modified_at ที่เพิ่มใน v2.1)
lineage_null = silver.filter(F.col("source_file").isNull() | F.col("source_file_modified_at").isNull()
                             | F.col("bronze_batch_id").isNull() | F.col("silver_batch_id").isNull()).count()
add("R5", "Lineage ครบทุกแถวใน Silver", "0 แถวที่ lineage ว่าง", f"{lineage_null} แถว", lineage_null == 0)

df_results = spark.createDataFrame(results, "case_id string, test_case string, expected string, actual string, status string") \
                  .withColumn("tested_at", F.current_timestamp())
df_results.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TEST_SCHEMA}.test_results")
display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. หลักฐานสำหรับรายงาน: quarantine แยกตามเหตุผล + row trace

# COMMAND ----------

display(quar.groupBy("_dq_reason").count().orderBy("_dq_reason"))
display(spark.table(PARAMS_SILVER["dq_run_log"]).orderBy(F.desc("run_started_at")).limit(10))

n_fail = df_results.filter("status != 'PASS'").count()
print(f"Test cases: {df_results.count()} | FAIL: {n_fail}")
if n_fail:
    raise AssertionError("DQ test มีกรณีที่ไม่ผ่าน — ดูตาราง test_results")
print("DQ mechanism test: ALL PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (ไม่บังคับ) ล้างตารางทดสอบ
# MAGIC เอา `#` ออกแล้วรัน cell ด้านล่างเมื่อไม่ต้องการเก็บหลักฐานแล้ว

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
# dbutils.fs.rm(TEST_DIR, recurse=True)