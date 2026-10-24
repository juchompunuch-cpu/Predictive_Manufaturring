# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Setup_Workflow — สร้าง / อัปเดต Databricks Workflow (Job) ด้วยโค้ด
# MAGIC
# MAGIC **v2 (23 ต.ค.):** dq_test รันขนานกับ bronze_autoloader (เดิมต่อกัน รวม ~22.5 นาที) · alert task retry 2 ครั้ง (กัน SQL Warehouse หยุดกลางคัน) · ค่าเริ่มต้น widget เป็น `false` ทั้งคู่ (อัปเดต Job อย่างเดียว ไม่ล้างข้อมูล ไม่สั่งรัน)
# MAGIC
# MAGIC รัน notebook นี้ครั้งเดียว (Run all) → ได้ Job **`Manufacturing_Defect_Pipeline`** ที่รันอัตโนมัติเมื่อมีไฟล์ CSV ใหม่ใน `landing/`
# MAGIC รันซ้ำได้ (idempotent): ถ้ามี Job ชื่อนี้อยู่แล้วจะอัปเดตค่าแทนการสร้างใหม่
# MAGIC
# MAGIC ```
# MAGIC  [file arrival: /Volumes/.../bronze_preditive/landing/]
# MAGIC          │
# MAGIC          ├──► dq_test (ทดสอบกลไก DQ) ─────────────────────────┐
# MAGIC          └──► bronze_autoloader ──► has_new_data? ──(true)──────┴──► silver ──► gold ──► dashboard ──► ml_predictive
# MAGIC              (Auto Loader)        (new_rows > 0)            (รอทั้งสองผ่าน)        │
# MAGIC                                                                             ├──► alert_high_defect
# MAGIC                                                                             └──► alert_quality_gate
# MAGIC  ถ้า task ใดล้มเหลว → อีเมลแจ้งเจ้าของ Job
# MAGIC ```
# MAGIC
# MAGIC | Task | Notebook / ประเภท | หน้าที่ |
# MAGIC |---|---|---|
# MAGIC | `dq_test` | DQ_Test_Dirty_Data | ทดสอบกลไก Data Quality ทุกรอบ (schema `dq_test`) **รันขนานกับ Bronze** เพื่อลดเวลา — Silver จะเริ่มเมื่อ DQ test ผ่านเท่านั้น |
# MAGIC | `bronze_autoloader` | Bronze_AutoLoader | รับเฉพาะไฟล์ใหม่จาก `landing/` แบบ append + reconciliation ทีละไฟล์ |
# MAGIC | `has_new_data` | Condition task | `new_rows > 0` ? ถ้าไม่มีแถวใหม่ → ข้าม task ที่เหลือ |
# MAGIC | `silver` → `gold` → `dashboard` | notebook v2.1 เดิม | คำนวณใหม่ทั้งชุดจาก Bronze, โหลด PostgreSQL, publish dashboard |
# MAGIC | `ml_predictive` | ML_Predictive | เทรนโมเดลใหม่จากข้อมูลล่าสุด |
# MAGIC | `alert_high_defect`, `alert_quality_gate` | SQL Alert เดิม 2 ตัว | ตรวจ KPI / quality gate หลัง Gold อัปเดต แล้วแจ้งเตือนทางอีเมล |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config

# COMMAND ----------

import os, json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

dbutils.widgets.dropdown("reset_bronze", "false", ["true", "false"])  # true เฉพาะครั้งแรก (ย้าย Bronze มาใช้ Auto Loader) — ค่าเริ่มต้น false กันล้างตารางโดยไม่ตั้งใจ
dbutils.widgets.dropdown("run_now", "false", ["true", "false"])

JOB_NAME     = "Manufacturing_Defect_Pipeline"
EMAIL        = w.current_user.me().user_name
NB_DIR       = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

CATALOG      = "predictive_manufacring_defect"
SRC_FILE     = "/Volumes/predictive_manufacring_defect/bronze_preditive/bronze_predic/manufacturing_defect_dataset.csv"
LANDING_DIR  = "/Volumes/predictive_manufacring_defect/bronze_preditive/landing/"
CHECKPOINT   = "/Volumes/predictive_manufacring_defect/bronze_preditive/checkpoints/bronze_autoloader"
BRONZE_TABLE = f"{CATALOG}.bronze_preditive.manufacturing_defect_dataset_v2"

ALERT_NAMES  = {"alert_high_defect": "High-defect % above target", "alert_quality_gate": "Quality gate failed"}

wh = next(x for x in w.warehouses.list())
WAREHOUSE_ID = wh.id
print("Notebook folder:", NB_DIR)
print("Owner / email  :", EMAIL)
print("Warehouse      :", wh.name, WAREHOUSE_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. เตรียม landing volume + ไฟล์ batch แรก

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze_preditive.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze_preditive.checkpoints")

existing = [f.name for f in dbutils.fs.ls(LANDING_DIR)]
if not any(n.endswith(".csv") for n in existing):
    dbutils.fs.cp(SRC_FILE, LANDING_DIR + "manufacturing_defect_batch_001.csv")
    print("Copied batch 001 into landing")
display(dbutils.fs.ls(LANDING_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ย้าย Bronze มาใช้ Auto Loader (ครั้งแรกเท่านั้น)
# MAGIC ตาราง Bronze เดิมถูกโหลดแบบ overwrite → ล้างตาราง + checkpoint เพื่อให้ Auto Loader สร้างใหม่จากไฟล์ใน `landing/` (ไม่ให้แถวซ้ำ)

# COMMAND ----------

if dbutils.widgets.get("reset_bronze") == "true":
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
    dbutils.fs.rm(CHECKPOINT, recurse=True)
    print("Bronze table + checkpoint reset → Auto Loader จะอ่านทุกไฟล์ใน landing ในรอบแรก")
else:
    print("Skip reset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. หา ID ของ SQL Alert เดิม 2 ตัว

# COMMAND ----------

def list_alerts():
    for path in ("/api/2.0/alerts", "/api/2.0/sql/alerts"):
        try:
            res = w.api_client.do("GET", path, query={"page_size": 100})
            items = res.get("alerts", res.get("results", res)) if isinstance(res, dict) else res
            if items:
                return items
        except Exception as e:
            print(f"list {path} failed: {str(e)[:120]}")
    return []

alerts = list_alerts()
ALERT_IDS = {}
for key, name in ALERT_NAMES.items():
    hit = next((a for a in alerts if a.get("display_name", a.get("name")) == name), None)
    if hit:
        ALERT_IDS[key] = hit["id"]
print("Alert IDs:", ALERT_IDS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. นิยาม Job (Jobs API 2.2)

# COMMAND ----------

def nb(task_key, notebook, depends=None, params=None, timeout=3600):
    t = {"task_key": task_key,
         "notebook_task": {"notebook_path": f"{NB_DIR}/{notebook}", "source": "WORKSPACE",
                           "base_parameters": params or {}},
         "timeout_seconds": timeout}
    if depends:
        t["depends_on"] = depends
    return t

CORE_TASKS = [
    nb("dq_test", "DQ_Test_Dirty_Data"),
    nb("bronze_autoloader", "Bronze_AutoLoader", None, {"full_refresh": "false"}),          # v2: รันขนานกับ dq_test
    {"task_key": "has_new_data",
     "depends_on": [{"task_key": "bronze_autoloader"}],
     "condition_task": {"op": "GREATER_THAN",
                        "left": "{{tasks.bronze_autoloader.values.new_rows}}", "right": "0"}},
    nb("silver", "Silver_Predictive_v2", [{"task_key": "has_new_data", "outcome": "true"},
                                          {"task_key": "dq_test"}]),                       # รอทั้งข้อมูลใหม่ + DQ test ผ่าน
    nb("gold", "Gold_Predictive", [{"task_key": "silver"}]),
    nb("dashboard", "Dashboard_Predictive", [{"task_key": "gold"}]),
    nb("ml_predictive", "ML_Predictive", [{"task_key": "dashboard"}]),
]

def alert_tasks(style):
    """style: 'alert_task' (SQL Alert รุ่นใหม่) หรือ 'sql_task' (รุ่นเดิม)"""
    out = []
    for key, aid in ALERT_IDS.items():
        if style == "alert_task":
            body = {"alert_task": {"alert_id": aid, "warehouse_id": WAREHOUSE_ID,
                                   "subscribers": [{"user_name": EMAIL}]}}
        else:
            body = {"sql_task": {"warehouse_id": WAREHOUSE_ID,
                                 "alert": {"alert_id": aid, "subscriptions": [{"user_name": EMAIL}]}}}
        # v2: retry — SQL Warehouse บน Free Edition อาจหยุดกลางคัน ("warehouse was stopped") → ลองใหม่อัตโนมัติ 2 ครั้ง
        out.append({"task_key": key, "depends_on": [{"task_key": "gold"}],
                    "max_retries": 2, "min_retry_interval_millis": 60000, "retry_on_timeout": True,
                    "timeout_seconds": 900, **body})
    return out

def settings(tasks, with_trigger=True):
    s = {
        "name": JOB_NAME,
        "description": "Manufacturing Defect Medallion pipeline: DQ test → Bronze (Auto Loader) → Silver → Gold (Delta + Lakebase) → Dashboard → ML + Alerts",
        "tasks": tasks,
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "email_notifications": {"on_failure": [EMAIL]},
        "tags": {"project": "manufacturing_defect", "layer": "medallion"},
    }
    if with_trigger:
        s["trigger"] = {"pause_status": "UNPAUSED",
                        "file_arrival": {"url": LANDING_DIR,
                                         "min_time_between_triggers_seconds": 60,
                                         "wait_after_last_change_seconds": 60}}
    return s

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create / Update Job
# MAGIC ลองแบบครบก่อน (file arrival + alert task) ถ้า API ไม่รองรับบางส่วนจะถอยไปแบบถัดไป และพิมพ์บอกว่าใช้แบบไหน

# COMMAND ----------

found = [j for j in w.jobs.list(name=JOB_NAME)]
job_id = found[0].job_id if found else None

def upsert(s):
    global job_id
    if job_id:
        w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": s})
    else:
        job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=s)["job_id"]
    return job_id

ATTEMPTS = [
    ("file arrival + alert_task", lambda: settings(CORE_TASKS + alert_tasks("alert_task"))),
    ("file arrival + sql_task alert", lambda: settings(CORE_TASKS + alert_tasks("sql_task"))),
    ("file arrival (ไม่มี alert task)", lambda: settings(CORE_TASKS)),
    ("ไม่มี trigger (Run now เอง)", lambda: settings(CORE_TASKS, with_trigger=False)),
]
used = None
for label, build in ATTEMPTS:
    try:
        upsert(build())
        used = label
        break
    except Exception as e:
        print(f"✗ {label}: {str(e)[:300]}")
if used is None:
    raise RuntimeError("สร้าง Job ไม่สำเร็จ — ดู error ด้านบน")

host = w.config.host.rstrip("/")
print(f"✓ Job ready ({used}) → {host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. รันรอบแรก (โหลด batch 001 ผ่าน Auto Loader)

# COMMAND ----------

if dbutils.widgets.get("run_now") == "true":
    run = w.jobs.run_now(job_id=job_id)
    print(f"Started run {run.run_id} → {host}/jobs/{job_id}/runs/{run.run_id}")
    print("ดูความคืบหน้าในหน้า Jobs & Pipelines (ประมาณ 10–15 นาที)")