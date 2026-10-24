# Databricks notebook source
# MAGIC %md
# MAGIC # Demo_Drop_New_Batch — จำลองไฟล์ batch ใหม่ เพื่อ demo Workflow
# MAGIC
# MAGIC รัน notebook นี้ระหว่างนำเสนอ → เขียนไฟล์ `manufacturing_defect_batch_002.csv` ลง `landing/` → **Job ถูกปลุกเองภายใน ~1–2 นาที** (file arrival trigger)
# MAGIC
# MAGIC ไฟล์นี้เป็น **ข้อมูลจำลองสำหรับ demo** (seed คงที่ ได้ไฟล์เดิมทุกครั้ง) สร้างจากแถวจริงแบบสุ่มแล้วขยับค่าเล็กน้อย (±2%) พร้อมใส่กรณีที่ pipeline ต้องจับได้:
# MAGIC
# MAGIC | ส่วนของไฟล์ | จำนวน | ผลที่คาด |
# MAGIC |---|---|---|
# MAGIC | แถวใหม่ (ค่าขยับจากแถวจริง) | 300 | เข้า Silver/Gold |
# MAGIC | แถวซ้ำกับ batch 001 ทุกคอลัมน์ (เช่น ส่งไฟล์ซ้ำบางส่วน) | 5 | quarantine: exact duplicate |
# MAGIC | แถวค่าผิด (QualityScore 150 / DefectStatus 2 / ProductionCost "abc") | 3 | quarantine ตาม rule |
# MAGIC
# MAGIC หลังรันจบ: Bronze 3,240 → **3,548** · Silver valid → **3,540** · quarantine → **8** (และ Alert "Quality gate failed" จะส่งอีเมลเพราะมีแถวเข้า quarantine)

# COMMAND ----------

import csv, io, random

SRC_FILE    = "/Volumes/predictive_manufacring_defect/bronze_preditive/bronze_predic/manufacturing_defect_dataset.csv"
LANDING_DIR = "/Volumes/predictive_manufacring_defect/bronze_preditive/landing/"
OUT_NAME    = "manufacturing_defect_batch_002.csv"

def make_batch(rows, seed=2026, n_new=300, n_dup=5):
    header, data = rows[0], rows[1:]
    col = {c: i for i, c in enumerate(header)}
    rnd = random.Random(seed)
    INT_COLS = {"ProductionVolume", "DeliveryDelay", "MaintenanceHours", "SafetyIncidents", "DefectStatus"}
    BOUNDS = {"ProductionVolume": (100, 999), "SupplierQuality": (80, 100), "DefectRate": (0.5, 5),
              "QualityScore": (60, 100), "DowntimePercentage": (0, 5), "StockoutRate": (0, 0.1),
              "WorkerProductivity": (80, 100), "EnergyEfficiency": (0.1, 0.5)}
    out = []
    for _ in range(n_new):
        r = rnd.choice(data)[:]
        for c, i in col.items():
            if c in ("DefectStatus", "DeliveryDelay", "MaintenanceHours", "SafetyIncidents"):
                continue                                       # คงค่าจำนวนเต็มเชิงหมวด/label เดิม
            v = float(r[i]) * (1 + rnd.uniform(-0.02, 0.02))
            lo, hi = BOUNDS.get(c, (0, float("inf")))
            v = min(max(v, lo), hi)
            r[i] = str(int(round(v))) if c in INT_COLS else repr(v)
        out.append(r)
    out += [rnd.choice(data)[:] for _ in range(n_dup)]          # แถวซ้ำกับ batch 001
    bad = [("QualityScore", "150"), ("DefectStatus", "2"), ("ProductionCost", "abc")]
    for c, v in bad:
        r = rnd.choice(data)[:]
        r[col[c]] = v
        out.append(r)
    return header, out

# COMMAND ----------

with open(SRC_FILE, encoding="utf-8-sig") as f:
    rows = list(csv.reader(f))
header, out = make_batch(rows)

buf = io.StringIO()
wr = csv.writer(buf, lineterminator="\n")
wr.writerow(header)
wr.writerows(out)
dbutils.fs.put(LANDING_DIR + OUT_NAME, buf.getvalue(), overwrite=False)
print(f"Wrote {len(out)} rows → {LANDING_DIR}{OUT_NAME}")
print("ไปที่ Jobs & Pipelines → Manufacturing_Defect_Pipeline เพื่อดู run ที่ถูก trigger")
display(dbutils.fs.ls(LANDING_DIR))