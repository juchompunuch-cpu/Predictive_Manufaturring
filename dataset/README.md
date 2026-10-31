# Dataset: Manufacturing Defect

| รายการ | ค่า |
|---|---|
| ไฟล์ | `manufacturing_defect_dataset.csv` |
| ขนาด | 3,240 แถว × 17 คอลัมน์ (มี header) |
| หน่วยของ 1 แถว | 1 production batch |
| Features | 16 ตัวแปรเชิงตัวเลขจาก 8 business domain (Production, Supply Chain, Quality, Maintenance, Inventory, Workforce/Safety, Energy, Additive Manufacturing) |
| Target | `DefectStatus`: 1 = High defect (2,723 / 84.04%), 0 = Low defect (517 / 15.96%) |
| Missing values | 0 |
| Duplicate rows | 0 |
| Business key | ไม่มี (pipeline สร้าง `record_id` = SHA-256 ของค่าทั้ง 17 คอลัมน์) |

ความหมายและช่วงค่าของแต่ละคอลัมน์: [docs/data_dictionary.md](../docs/data_dictionary.md)

## ข้อสังเกต
- Target ไม่สมดุล: ถ้าเดาว่า "เสียทุก batch" จะถูกแล้ว 84%
- ข้อมูลมีลักษณะคล้าย synthetic data: label `DefectStatus` ตรงกับกฎ threshold 4 ข้อ (MaintenanceHours > 10, ProductionVolume > 800, DefectRate > 3, QualityScore < 75) ถึง 96.11%
- ไม่มีวันที่หรือสายการผลิต จึงวิเคราะห์เชิงเวลาไม่ได้

## แหล่งที่มา
- **Dataset:** [🏭 Predicting Manufacturing Defects Dataset](https://www.kaggle.com/datasets/rabieelkharoua/predicting-manufacturing-defects-dataset), Kaggle (uploader: `rabieelkharoua`)
- **License:** ดูเงื่อนไขการใช้งานที่หน้า Kaggle ของ dataset ก่อนนำไปใช้ต่อ (MIT License ของ repo นี้ครอบคลุมเฉพาะโค้ดและเอกสาร ไม่ครอบคลุมข้อมูล)
- หน้า Kaggle ติด tag ว่าเป็น synthetic data ซึ่งสอดคล้องกับผลวิเคราะห์ว่า label สร้างจากกฎ threshold
