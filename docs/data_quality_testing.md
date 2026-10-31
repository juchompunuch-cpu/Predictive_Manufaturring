# Data Quality & ETL Testing

## 1. Data Quality Rules (Silver)

### Hard error → Quarantine

| Rule | Condition |
|---|---|
| Required | `is_high_defect`, `production_volume`, `defect_rate` IS NULL |
| Target domain | `is_high_defect` ไม่ใช่ 0/1 |
| Percentage | `supplier_quality`, `quality_score`, `downtime`, `worker_productivity` < 0 หรือ > 100 |
| Ratio 0–1 | `stockout_rate`, `energy_efficiency_factor` < 0 หรือ > 1 |
| Non-negative | 10 คอลัมน์ปริมาณ/ต้นทุน < 0 |
| Max hours | `maintenance_hours_per_week` > 168 |
| NaN | คอลัมน์ DOUBLE เป็น NaN |
| Cast error | แปลงชนิดไม่ได้ / INT มีเศษ |

ทุก rule คืนค่า NULL เมื่อผ่าน และคืนข้อความเมื่อไม่ผ่าน แล้วรวมเป็น array (`_dq_errors`) แถวที่ `_dq_reason IS NULL` คือ valid

### Business-rule warning (ติดธง ไม่ตัดแถว)
- `production_volume = 0` แต่มีต้นทุน/พลังงาน
- `defect_rate > 4.5` แต่ label = 0
- `quality_score < 60`

### Missing values
ตรวจ placeholder 10 แบบ (`''`, `null`, `none`, `nan`, `n/a`, `na`, `-`, `?`, `missing`, `undefined`) แบบไม่สนตัวพิมพ์ แล้วแปลงเป็น NULL
- คอลัมน์บังคับว่าง → Quarantine พร้อมเหตุผล
- คอลัมน์อื่นว่าง → เก็บแถว ค่าเป็น NULL (ไม่เดาค่าเอง)

ผลกับข้อมูลจริง: placeholder 0 ค่า, NULL 0 ค่า จึงไม่ต้องลบแถวหรือเติมค่า

### Silver Quality Gate
- valid rows > 0
- คอลัมน์ครบตาม `FINAL_COLUMNS`
- `is_high_defect` ไม่เป็น NULL และเป็น 0/1 เท่านั้น
- `record_id` ไม่เป็น NULL และไม่ซ้ำ

หลังเขียนตาราง เพิ่ม Delta CHECK constraints 9 ข้อ เช่น `chk_target_binary`, `chk_stockout_ratio`, `chk_maint_hours`

## 2. สรุปผลการทดสอบ

| หมวด | การทดสอบ | ผล |
|---|---|---|
| Schema validation | header = 17 คอลัมน์ที่คาดไว้ | PASS |
| Data type validation | `try_cast` ทุกคอลัมน์, INT ต้องไม่มีเศษ | cast error 0 → PASS |
| Missing value | placeholder 10 แบบ + NULL profile | 0 ค่า → PASS |
| Duplicate | `record_id` SHA-256 + `row_number` | ซ้ำ 0 → PASS |
| DQ | DQ rules + Delta CHECK 9 ข้อ + quality gate | fail 0 → PASS |
| PK/FK (Spark) | PK null/ซ้ำ, FK orphan ใน 6 ตาราง | PASS ทุกตาราง |
| PK/FK (PostgreSQL) | orphan 5 ตาราง + row count 6 ตาราง + negative test 3 ข้อ | 14/14 PASS |
| Row count reconciliation | ไฟล์ = Bronze = Silver = Gold = PostgreSQL | 3,240 ทุกชั้น |
| Dirty data test | 15 กรณี + reconciliation/lineage 5 ข้อ | 19/19 PASS |
| End-to-end automation | ไฟล์ batch ใหม่ 308 แถว | PASS |

## 3. Negative test ใน PostgreSQL

| Test | คาดหวัง | ผลจริง |
|---|---|---|
| INSERT `quality` ด้วย `production_id = -999` | Reject | SQLSTATE 23503 `foreign_key_violation` |
| INSERT `production` ด้วย `production_id = 1` (ซ้ำ) | Reject | SQLSTATE 23505 `unique_violation` |
| INSERT `quality` ด้วย `is_high_defect = 5` | Reject | ถูกปฏิเสธ |

## 4. Dirty Data Test (`DQ_Test_Dirty_Data`)

สร้างไฟล์ทดสอบจากแถวจริง 200 แถว + แถวผิด 15 กรณี (แก้ค่าเพียง 1 จุดต่อกรณี) แล้วเรียก `Bronze_Predictive_v2` และ `Silver_Predictive_v2` ตัวจริงผ่าน `dbutils.notebook.run` โดยเขียนลง schema `dq_test` (ไม่ทับตารางจริง) และบันทึกผลที่ `dq_test.test_results`

| Case | ข้อมูลที่ฉีดเข้าไป | ผลที่คาด | Status |
|---|---|---|---|
| T01 | `'N/A'` ใน DefectRate (บังคับ) | quarantine: required value is NULL | PASS |
| T02 | ค่าว่างใน DefectStatus | quarantine: required value is NULL | PASS |
| T03 | `'abc'` ใน ProductionCost | quarantine: invalid value for double | PASS |
| T04 | 12.5 ใน MaintenanceHours (INT) | quarantine: non-integer value for int | PASS |
| T05 | DefectStatus = 2 | quarantine: value must be 0 or 1 | PASS |
| T06 | QualityScore = 150 | quarantine: between 0 and 100 | PASS |
| T07 | StockoutRate = 5 | quarantine: between 0 and 1 | PASS |
| T08 | ProductionVolume = -10 | quarantine: cannot be negative | PASS |
| T09 | MaintenanceHours = 200 | quarantine: cannot exceed 168 | PASS |
| T10 | แถวซ้ำทุกคอลัมน์ | quarantine: exact duplicate | PASS |
| T11 | `'N/A'` ใน EnergyEfficiency (ไม่บังคับ) | valid, ค่าเป็น NULL | PASS |
| T12 | `' 512 '` | valid, trim เป็น 512 | PASS |
| T13 | defect_rate 4.8 แต่ label = 0 | valid + business warning | PASS |
| T14–T15 | แถวคอลัมน์เกิน / ขาด | Bronze corrupt 2 → quarantine 2 | PASS |
| R1 | Bronze rows = บรรทัดในไฟล์ | 215 = 215 | PASS |
| R2 | Silver valid rows | 203 | PASS |
| R3 | Quarantine rows | 12 | PASS |
| R4 | valid + quarantine = Bronze | 203 + 12 = 215 | PASS |
| R5 | Lineage ครบทุกแถว | 0 แถวที่ว่าง | PASS |

Row trace: Bronze 215 → แยก format เสีย 2 → 213 → dedup → 212 → valid 203 + ไม่ผ่าน DQ 9 → quarantine รวม 12 (DQ 9 + ซ้ำ 1 + format เสีย 2)

## 5. End-to-End Automation Test (batch_002, 308 แถว)

| รายการตรวจ | ผลที่คาด | ผลจริง | Status |
|---|---|---|---|
| Trigger | Job เริ่มเองเมื่อมีไฟล์ใหม่ | Launched: By file arrival | PASS |
| Auto Loader | อ่านเฉพาะไฟล์ใหม่ 308 แถว | `new_rows = 308` | PASS |
| Bronze | 3,240 + 308 = 3,548 | 3,548 | PASS |
| Silver valid | 3,240 + 300 = 3,540 | 3,540 | PASS |
| Quarantine | ซ้ำ 5 + ค่าผิด 3 = 8 | 8 | PASS |
| valid + quarantine = Bronze | 3,540 + 8 = 3,548 | ตรงกัน | PASS |
| Gold / PostgreSQL / Dashboard | 3,540 batch | Dashboard แสดง 3.54K | PASS |
| SQL Alert quality gate | TRIGGERED | อีเมล TRIGGERED | PASS |
| Reset (full refresh) | 3,240 / 3,240 / 0 | 3,240 / 3,240 / 0 | PASS |

### เวลาที่ใช้ต่อ task

| Task | รอบ demo (file arrival) | รอบ reset (full refresh) |
|---|---|---|
| dq_test | 12m 49s | 9m 45s |
| bronze_autoloader | 32.6s | 32s |
| has_new_data | 1.2s | – |
| silver | 1m 11s | 1m 11s |
| gold | 3m 9s | 3m 15s |
| alert_high_defect / alert_quality_gate | 4m 37s | 4m 48s (Failed: warehouse stopped) |
| dashboard | 9.8s | 11s |
| ml_predictive | 1m 7s | 1m 11s |
| **รวม** | **22m 33s (Succeeded)** | 29m 32s (Failed เฉพาะ alert) |

## 6. Monitoring tables

| ตาราง | เก็บอะไร |
|---|---|
| `bronze_preditive.ingestion_log` | batch_id, source_file, source_rows, loaded_rows, corrupt_rows, status (PASS / FAIL / SCHEMA_DRIFT) |
| `silver_predictive.dq_run_log` | row trace ทุกขั้นของ Silver ต่อ `silver_batch_id` |
| `gold_predictive.gold_run_log` | จำนวนแถวที่โหลดเข้า PostgreSQL ต่อตาราง ต่อ `gold_batch_id` |
| `dq_test.test_results` | ผลทดสอบกลไก DQ ทีละกรณี |
| Job run history / SQL Alerts + email | สถานะ task, trigger, alert TRIGGERED และ on_failure |
