# Data Dictionary

Manufacturing Defect Dataset: 3,240 แถว × 17 คอลัมน์. 1 แถว = 1 production batch

ชื่อคอลัมน์ถูกเปลี่ยนจาก CamelCase เป็น snake_case ในชั้น Silver พร้อมระบุหน่วยในชื่อ (ก่อน rename มีการตรวจว่าคอลัมน์ต้นทางครบ)

| คอลัมน์ต้นทาง | ชื่อใน Silver / Gold | ความหมาย | Type | Domain | Gold table | ช่วงค่าจริง | บทบาท |
|---|---|---|---|---|---|---|---|
| ProductionVolume | `production_volume` | จำนวนหน่วยที่ผลิตต่อ batch | INT | Production | production | 100 – 999 | Controllable |
| ProductionCost | `production_cost` | ต้นทุนการผลิต (USD) | DOUBLE | Production | production | 5,000 – 19,993 | – |
| SupplierQuality | `supplier_quality_pct` | คะแนนคุณภาพ supplier (%) | DOUBLE | Supply Chain | supply_chain | 80.0 – 100.0 | Controllable |
| DeliveryDelay | `delivery_delay_days` | ความล่าช้าในการส่งวัตถุดิบ (วัน) | INT | Supply Chain | supply_chain | 0 – 5 | Controllable |
| DefectRate | `defect_rate` | อัตราของเสียต่อ 1,000 หน่วย | DOUBLE | Quality | quality | 0.50 – 5.00 | Outcome |
| QualityScore | `quality_score_pct` | คะแนนคุณภาพ (%) | DOUBLE | Quality | quality | 60.0 – 100.0 | Outcome |
| MaintenanceHours | `maintenance_hours_per_week` | ชั่วโมงซ่อมบำรุงต่อสัปดาห์ | INT | Maintenance | maintenance | 0 – 23 | Controllable |
| DowntimePercentage | `downtime_pct` | เวลาหยุดเครื่อง (%) | DOUBLE | Maintenance | maintenance | 0.00 – 5.00 | Controllable |
| InventoryTurnover | `inventory_turnover_ratio` | อัตราหมุนเวียนสินค้าคงคลัง | DOUBLE | Inventory | inventory | 2.0 – 10.0 | – |
| StockoutRate | `stockout_rate` | สัดส่วนสินค้าขาดสต็อก (0–1) | DOUBLE | Inventory | inventory | 0.00 – 0.10 | Controllable |
| WorkerProductivity | `worker_productivity_pct` | ผลิตภาพแรงงาน (%) | DOUBLE | Workforce | operations | 80.0 – 100.0 | Controllable |
| SafetyIncidents | `safety_incidents_count` | จำนวนอุบัติเหตุ | INT | Workforce | operations | 0 – 9 | – |
| EnergyConsumption | `energy_consumption_kwh` | พลังงานที่ใช้ (kWh) | DOUBLE | Energy | operations | 1,001 – 4,997 | – |
| EnergyEfficiency | `energy_efficiency_factor` | ประสิทธิภาพพลังงาน (0–1) | DOUBLE | Energy | operations | 0.10 – 0.50 | Controllable |
| AdditiveProcessTime | `additive_process_time_hrs` | เวลา additive process (ชม.) | DOUBLE | Additive Mfg. | operations | 1.0 – 10.0 | – |
| AdditiveMaterialCost | `additive_material_cost_usd` | ต้นทุนวัสดุ additive (USD) | DOUBLE | Additive Mfg. | operations | 100 – 500 | – |
| DefectStatus | `is_high_defect` | Target: 1 = high defect, 0 = low defect | INT (0/1) | Quality | quality | 0 / 1 | Target |

## คอลัมน์ที่ pipeline สร้างเพิ่ม

| คอลัมน์ | ชั้น | ความหมาย |
|---|---|---|
| `_source_file`, `_source_file_modified_at`, `_ingested_at`, `_bronze_batch_id` | Bronze | ingestion metadata |
| `_corrupt_record` | Bronze | แถวที่ parse ไม่ได้ (mode PERMISSIVE) |
| `record_id` | Silver / Gold | SHA-256 ของค่าธุรกิจทั้ง 17 คอลัมน์ (natural key, UNIQUE) |
| `source_table`, `source_file`, `source_file_modified_at`, `bronze_batch_id`, `bronze_ingested_at`, `silver_processed_at`, `silver_batch_id` | Silver | lineage |
| `_dq_warning_reason` / `dq_warning_reason` | Silver / Gold | business-rule warning (ไม่ตัดแถว) |
| `production_id` | Gold | surrogate key BIGINT (`row_number` เรียงตาม `record_id`) |
| `cost_per_unit`, `energy_kwh_per_unit` | Gold | ค่าคำนวณต่อหน่วย |
| `*_band` | Gold | การจัดกลุ่มตามจุดเปลี่ยนจาก threshold scan |
| `risk_flag_count`, `risk_segment`, `qc_alert`, `label_rule_pred` | Gold (`batch_360`) | flags สำหรับ dashboard / ML |
