# Databricks notebook source
# MAGIC %md
# MAGIC # Reset_Demo — รีเซ็ตกลับเป็นสถานะก่อน demo (batch 001 = 3,240 แถว)
# MAGIC
# MAGIC ใช้หลังซ้อม demo เพื่อให้ **ตัวเลขกลับมาตรงกับรายงาน** และวันนำเสนอสามารถรัน `Demo_Drop_New_Batch` ได้อีกครั้ง
# MAGIC
# MAGIC | ขั้น | ทำอะไร | ทำไม |
# MAGIC |---|---|---|
# MAGIC | 1 | **ย้าย** (ไม่ลบ) `manufacturing_defect_batch_002.csv` จาก `landing/` ไป `demo_archive/` | ให้ `landing/` เหลือแค่ batch 001 และเก็บไฟล์ไว้ตรวจย้อนได้ (การย้ายไฟล์ออกไม่ปลุก Job) |
# MAGIC | 2 | สั่งรัน Job 1 รอบด้วย `full_refresh = true` | Bronze_AutoLoader ล้างตาราง + checkpoint แล้วอ่านไฟล์ใน `landing/` ใหม่ทั้งหมด → Silver / Gold / PostgreSQL / Dashboard / ML คำนวณใหม่จาก 3,240 แถว |
# MAGIC | 3 | ตรวจจำนวนแถว (หลัง Job จบ) | ยืนยันว่า Bronze = Silver = 3,240 และ quarantine = 0 |
# MAGIC
# MAGIC กด **Run all ครั้งเดียว** เพื่อสั่ง reset → รอ Job จบ (ปกติ 15–40 นาที) → กลับมารัน **cell ข้อ 0 (Config) และข้อ 3** เพื่อตรวจผล (ไม่ต้อง Run all ซ้ำ)
# MAGIC
# MAGIC v4: (1) ถ้ามี run ของ Job ค้างอยู่ จะ **ไม่สั่งรันซ้ำ** (กันกด Run all 2 ครั้งแล้ว run ต่อคิวกัน) (2) notebook **ไม่นั่งรอ** Job เพราะ Free Edition จำกัดจำนวน serverless compute — notebook ที่รออยู่จะแย่งโควตา ทำให้ SQL Warehouse ของ alert เปิดไม่ได้ (RESOURCE_EXHAUSTED) และ task ช้าลง

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

dbutils.widgets.dropdown("run_job", "true", ["true", "false"])

JOB_NAME     = "Manufacturing_Defect_Pipeline"
CATALOG      = "predictive_manufacring_defect"
LANDING_DIR  = "/Volumes/predictive_manufacring_defect/bronze_preditive/landing/"
ARCHIVE_DIR  = "/Volumes/predictive_manufacring_defect/bronze_preditive/landing_archive/"
DEMO_FILE    = "manufacturing_defect_batch_002.csv"
BRONZE_TABLE = f"{CATALOG}.bronze_preditive.manufacturing_defect_dataset_v2"
SILVER_TABLE = f"{CATALOG}.silver_predictive.manufacturing_defect_dataset_v2"
QUAR_TABLE   = f"{CATALOG}.silver_predictive.manufacturing_defect_dataset_quarantine_v2"

def task_states(run_id):
    r = w.jobs.get_run(run_id)
    for t in r.tasks or []:
        st = t.state.result_state or t.state.life_cycle_state
        print(f"  {t.task_key:<20} {st}")
    return r

def counts(label):
    b = spark.table(BRONZE_TABLE).count()
    s = spark.table(SILVER_TABLE).count()
    q = spark.table(QUAR_TABLE).count()
    print(f"{label:<8} Bronze={b:,}  Silver={s:,}  Quarantine={q:,}")
    return b, s, q

counts("ก่อน")
display(dbutils.fs.ls(LANDING_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ย้ายไฟล์ demo ออกจาก landing (เก็บไว้ใน landing_archive)
# MAGIC เก็บไว้นอก `landing/` (คนละ volume) เพื่อให้ Auto Loader ไม่อ่านไฟล์นี้อีก

# COMMAND ----------

from datetime import datetime

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze_preditive.landing_archive")

names = [f.name for f in dbutils.fs.ls(LANDING_DIR)]
if DEMO_FILE in names:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = f"{ARCHIVE_DIR}{stamp}_{DEMO_FILE}"
    dbutils.fs.mv(LANDING_DIR + DEMO_FILE, target)
    print("Moved →", target)
else:
    print("ไม่มีไฟล์ demo ใน landing (อาจรีเซ็ตไปแล้ว)")

display(dbutils.fs.ls(LANDING_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. รัน Job 1 รอบแบบ full refresh
# MAGIC `notebook_params` ส่ง `full_refresh=true` ให้ task `bronze_autoloader` เฉพาะรอบนี้ (ค่าใน Job ยังเป็น `false` เหมือนเดิม)

# COMMAND ----------

job = next(iter(w.jobs.list(name=JOB_NAME)), None)
assert job, f"ไม่พบ Job {JOB_NAME} — รัน Setup_Workflow ก่อน"

host = w.config.host.rstrip("/")

# ถ้ามี run ค้างอยู่แล้ว (เช่น เคยกด Run all ไปแล้ว) → ไม่สั่งรันซ้ำ
active = list(w.jobs.list_runs(job_id=job.job_id, active_only=True))
if active:
    RUN_ID = active[0].run_id
    print(f"มี run กำลังทำงานอยู่แล้ว → ไม่สั่งรันซ้ำ (run {RUN_ID})")
elif dbutils.widgets.get("run_job") == "true":
    RUN_ID = w.jobs.run_now(job_id=job.job_id,
                            notebook_params={"full_refresh": "true"}).run_id
    print(f"Started reset run {RUN_ID}")
else:
    RUN_ID = None
    print("ข้ามการรัน Job (run_job = false)")

if RUN_ID:
    print(f"→ {host}/jobs/{job.job_id}/runs/{RUN_ID}")
    print("Job ใช้เวลาประมาณ 15–40 นาที — notebook นี้ **ไม่รอ** (v4) เพื่อไม่กินโควตา serverless ของ Free Edition")
    print("ระหว่างรอ: ปิด/Terminate compute ของ notebook นี้ได้เลย แล้วกลับมารัน cell ข้อ 0 (Config) + ข้อ 3 เมื่อ Job จบ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ตรวจผลหลัง Job จบ
# MAGIC คาดว่า: Bronze = 3,240 · Silver = 3,240 · Quarantine = 0

# COMMAND ----------

job = next(iter(w.jobs.list(name=JOB_NAME)), None)
last = next(iter(w.jobs.list_runs(job_id=job.job_id, limit=1)), None)
if last:
    print(f"run ล่าสุด {last.run_id}: {last.state.life_cycle_state} {last.state.result_state or ''}")
    task_states(last.run_id)
    if last.state.life_cycle_state.value not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        print("⚠️ Job ยังรันอยู่ — ตัวเลขด้านล่างอาจยังไม่ใช่ค่าสุดท้าย รอให้ run จบแล้วรัน cell นี้ใหม่")
b, s, q = counts("หลัง")
if (b, s, q) == (3240, 3240, 0):
    print("Reset สำเร็จ: ตัวเลขตรงกับรายงานแล้ว และพร้อมรัน Demo_Drop_New_Batch อีกครั้ง")
else:
    print("ยังไม่ตรง — ถ้า Job ยังรันไม่จบให้รอแล้วรัน cell นี้ใหม่")
