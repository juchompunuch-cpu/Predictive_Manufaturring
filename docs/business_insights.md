# Business Insights

แหล่งข้อมูล: `gold_predictive.kpi_overview`, `mart_threshold_scan`, `mart_driver_impact`, `mart_driver_bands`, `mart_risk_segment`, `mart_label_rule`

## KPI (kpi_overview)

| KPI | ค่า | KPI | ค่า |
|---|---|---|---|
| Total batches | 3,240 | High-defect batches | 2,723 (84.04%) |
| Total units | 1,777,215 | Avg defect rate | 2.749 |
| Total production cost | $40,250,579.86 | Avg cost per unit | $22.65 |
| Cost in high-defect batches | 84.38% | No controllable flag | 34.88% |
| Avg quality score | 80.13 | Avg supplier quality | 89.83 |
| Avg maintenance hours/week | 11.48 | Avg downtime | 2.50% |
| Avg stockout | 5.09% | Avg worker productivity | 90.04% |
| Total safety incidents | 14,877 | Total energy | 9,682,722 kWh |
| QC alert batches | 63.80% | Label = threshold rule | 96.11% |

## Threshold scan: ผลของแต่ละตัวแปร

| ตัวแปร | บทบาท | เกณฑ์ | ฝั่งเสี่ยง % | ฝั่งปลอดภัย % | Gap (pp) | ระดับ |
|---|---|---|---|---|---|---|
| Maintenance hours/week | Controllable | > 10 | 95.62 | 69.89 | 25.73 | Strong |
| Production volume | Controllable | > 800 | 96.66 | 80.44 | 16.22 | Strong |
| Stockout rate | Controllable | > 0.01 | 84.86 | 76.43 | 8.43 | Moderate |
| Energy efficiency | Controllable | < 0.48 | 84.30 | 79.14 | 5.16 | Weak |
| Supplier quality | Controllable | > 83 | 84.64 | 80.80 | 3.84 | Weak |
| Downtime | Controllable | > 4.7 | 87.24 | 83.84 | 3.41 | Weak |
| Worker productivity | Controllable | < 99 | 84.13 | 82.31 | 1.81 | Weak |
| Delivery delay | Controllable | > 2 | 84.68 | 83.34 | 1.34 | Weak |
| Defect rate | Outcome | > 3 | 96.18 | 74.35 | 21.83 | Strong |
| Quality score | Outcome | < 75 | 95.80 | 77.22 | 18.58 | Strong |

Stockout rate ได้ระดับ Moderate เพราะฝั่งปลอดภัย (≤ 0.01) มีเพียงประมาณ 10% ของ batch

## High-defect % ตาม band

| ปัจจัย | Band (batches → high-defect %) |
|---|---|
| Maintenance hours/week | 0–5 h (810) 70.99% · 6–10 h (648) 68.52% · 11–16 h (842) 94.77% · 17–23 h (940) 96.38% |
| Production volume | 100–300 (739) 81.60% · 301–500 (729) 80.38% · 501–700 (668) 79.04% · 701–800 (385) 80.78% · 801+ (719) 96.66% |
| ต้นทุน/หน่วย (ตาม volume band) | $61.70 · $30.70 · $20.50 · $17.50 · $13.82 |
| Defect rate | 0.5–1 (366) 74.59% · 1–2 (730) 76.30% · 2–3 (705) 72.20% · 3–4 (699) 96.57% · 4–5 (740) 95.81% |
| Quality score | 60–75 (1,190) 95.80% · 75–80 (406) 77.59% · 80–90 (802) 76.81% · 90–100 (842) 77.43% |

## Risk segment × QC alert (Operating window)

Controllable flags: maintenance > 10, volume > 800 · QC alert: defect rate > 3 หรือ quality score < 75

| Segment | Batches (%) | High-defect % | QC ผ่าน: batch (HD%) | QC เตือน: batch (HD%) |
|---|---|---|---|---|
| Low (0 flag) | 1,130 (34.88%) | 62.04 | **417 (3.12%)** | 713 (96.49%) |
| Medium (1 flag) | 1,719 (53.06%) | 95.70 | 611 (95.74%) | 1,108 (95.67%) |
| High (2 flags) | 391 (12.07%) | 96.42 | 145 (95.17%) | 246 (97.15%) |

## Unknown Question: label นิยามจากอะไร

| ผลเทียบกฎ 4 ข้อกับ label | Batches | % |
|---|---|---|
| กฎตรงกับ label | 3,114 | 96.11 |
| เกิน threshold แต่ label = 0 | 113 | 3.49 |
| label = 1 แต่ไม่เกิน threshold ใดเลย | 13 | 0.40 |

## Insight → Action

| # | Insight | Business meaning | Action |
|---|---|---|---|
| 1 | 84.04% ของ batch เป็น high-defect (84.38% ของต้นทุน) | ปัญหาคุณภาพเป็นเรื่องทั้งโรงงาน | ตั้ง high-defect % เป็น KPI หลัก |
| 2 | Maintenance > 10 → 95.6% (vs 69.9%) | เครื่องที่ซ่อมบ่อยคือเครื่องไม่เสถียร (อาการ ไม่ใช่สาเหตุ) | Predictive maintenance / overhaul เครื่องกลุ่มนี้ก่อน |
| 3 | Volume > 800 → 96.7% (vs 80.4%) แต่ต้นทุน/หน่วยต่ำสุด | Trade-off ต้นทุนกับคุณภาพ | เพิ่ม QC sampling แทนการลดขนาด |
| 4 | Defect rate > 3 → 96.2% · Quality score < 75 → 95.8% | เป็นตัววัดผล ไม่ใช่คันโยก | SPC alert ที่ 3.0, QC gate ที่ 75 |
| 5 | Supplier, delay, downtime, stockout, productivity, energy ต่างกันไม่เกิน 8.4 pp | ไม่ใช่ต้นเหตุหลักของ defect | ทบทวนงบที่อาจไม่คุ้ม |
| 6 | ไม่มี flag + QC ผ่าน → 3.12% (417 batch) | มี operating window ที่ปลอดภัย | ใช้ 2 controllable flags เป็น risk score + QC alert |
| 7 | กฎ 4 ข้อตรงกับ label 96.11% | label น่าจะสร้างจากกฎ | ยืนยันนิยาม label กับเจ้าของข้อมูลก่อนตัดสินใจ |

> ข้อควรระวัง: ผลทั้งหมดเป็น **ความสัมพันธ์ ไม่ใช่เหตุและผล** และ threshold ได้จากการ scan หลายจุด ควรยืนยันกับข้อมูลชุดใหม่
