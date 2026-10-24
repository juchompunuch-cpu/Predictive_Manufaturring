# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Dashboard_Predictive — Business Insight & AI/BI Dashboard (จาก Gold layer)
# MAGIC
# MAGIC สร้าง/อัปเดต **Databricks AI/BI Dashboard** ด้วยโค้ด (reproducible) จากตาราง Gold:
# MAGIC - **Delta** `predictive_manufacring_defect.gold_predictive.*` : kpi_overview, mart_driver_impact, mart_driver_bands, mart_maint_hours_curve, mart_risk_matrix, mart_action_matrix, mart_label_rule
# MAGIC - **Lakebase PostgreSQL** (ผ่าน UC catalog `manufacturing_gold_pg.gold`) : `vw_kpi_overview` (counter จำนวน batch และ high-defect %) และ `vw_risk_segment` (กราฟ Q8)
# MAGIC
# MAGIC ## Business Questions
# MAGIC | # | คำถาม | ประเภทตัวแปรที่ใช้ตอบ |
# MAGIC |---|---|---|
# MAGIC | 1 | สถานการณ์คุณภาพโดยรวม และกระทบต้นทุนเท่าไร | KPI |
# MAGIC | 2 | ปัจจัยไหน **ที่ทีมปรับได้** สัมพันธ์กับ high-defect มากที่สุด | Controllable vs Outcome |
# MAGIC | 3 | ชั่วโมงซ่อมบำรุงมีผลอย่างไร จุดเปลี่ยนอยู่ตรงไหน | Controllable |
# MAGIC | 4 | Defect rate ระดับไหนควรเตือน (QC monitoring) | Outcome |
# MAGIC | 5 | Quality score ระดับไหนควรเตือน (QC monitoring) | Outcome |
# MAGIC | 6 | batch ใหญ่เสี่ยงกว่าหรือไม่ | Controllable |
# MAGIC | 7 | ปัจจัยไหน *ไม่* ช่วยอธิบาย defect | Controllable |
# MAGIC | 8 | มี operating window ที่ปลอดภัยหรือไม่ (ก่อนผลิต + หลังผลิต) | Controllable × Outcome |
# MAGIC | 9 | **Unknown Question:** label `is_high_defect` นิยามจากอะไร | ทุกตัว |
# MAGIC
# MAGIC กล่อง *Key insights* ดึงตัวเลขจากตาราง Gold ตอนรัน → ตัวเลขใน dashboard ตรงกับข้อมูลเสมอ

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

w = WorkspaceClient()
FOLDER    = "/Users/69056012@kmitl.ac.th/Predictive_Manufaturring/Predictive_Manu"
DASH_NAME = "Manufacturing Defect - Gold Insights"
G  = "predictive_manufacring_defect.gold_predictive"
PG = "manufacturing_gold_pg.gold"          # Lakebase PostgreSQL (UC-registered)

wh = next(x for x in w.warehouses.list())
WAREHOUSE_ID = wh.id
print("Warehouse:", wh.name, WAREHOUSE_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ดึงตัวเลขสำหรับกล่อง insight จาก Gold

# COMMAND ----------

kpi    = spark.table(f"{G}.kpi_overview").first().asDict()
scan   = {r["driver"]: r.asDict() for r in spark.table(f"{G}.mart_threshold_scan").collect()}
seg    = {r["risk_segment"]: r.asDict() for r in spark.table(f"{G}.mart_risk_segment").collect()}
action = {(r["risk_segment"], r["qc_alert"]): r.asDict() for r in spark.table(f"{G}.mart_action_matrix").collect()}

key_ctrl = sorted([v for v in scan.values() if v["driver_role"] == "Controllable" and v["is_key_factor"]], key=lambda v: -v["gap_pp"])
key_out  = sorted([v for v in scan.values() if v["driver_role"] == "Outcome" and v["is_key_factor"]], key=lambda v: -v["gap_pp"])
weak     = sorted(v["driver"] for v in scan.values() if v["driver_role"] == "Controllable" and not v["is_key_factor"])
max_weak = max(v["gap_pp"] for v in scan.values() if v["driver_role"] == "Controllable" and not v["is_key_factor"])

def rule(v):   return f"{v['driver']} {v['rule']}"
def side(v):   return f"{v['risky_side_high_defect_pct']:.1f}% vs {v['safe_side_high_defect_pct']:.1f}%"

low_pass  = action.get(("Low", 0), {})
low_alert = action.get(("Low", 1), {})
INSIGHTS = [
    "### Key insights & actions",
    f"1. **{kpi['high_defect_pct']}% ของ batch เป็น high-defect** และคิดเป็น {kpi['cost_share_in_high_defect_pct']}% ของต้นทุน → ตั้ง high-defect % เป็น KPI หลัก",
    "2. **ปัจจัยที่ทีมปรับได้ (Controllable)** ที่มีผลชัด: " + " · ".join(f"**{rule(v)}** → {side(v)}" for v in key_ctrl),
    "3. **Maintenance hours** มีจุดกระโดด ไม่ใช่ค่อย ๆ เพิ่ม → ทำ predictive maintenance / overhaul เครื่องที่ต้องซ่อมเกิน threshold ก่อน (ชั่วโมงซ่อมสูงอาจเป็น *อาการ* ของเครื่องไม่เสถียร ไม่ใช่สาเหตุ)",
    "4. **Batch ใหญ่** เสี่ยงขึ้นเฉพาะช่วงบนสุด แต่ต้นทุน/หน่วยต่ำกว่า → เพิ่ม QC sampling ใน batch ใหญ่แทนการลดขนาด",
    "5. **Defect rate / Quality score เป็นตัววัดผล (Outcome)** ไม่ใช่คันโยก → ใช้เป็น **QC alert**: " + " · ".join(f"{rule(v)}" for v in key_out),
    f"6. {', '.join(weak)} → ต่างกันไม่เกิน {max_weak:.1f} pp → **ไม่ใช่คันโยกหลัก**",
    f"7. **Operating window:** ไม่มี controllable flag + QC ผ่าน ({low_pass.get('batches', 0)} batch) → high-defect เพียง **{low_pass.get('high_defect_pct', 0)}%** · ไม่มี flag แต่ QC เตือน → {low_alert.get('high_defect_pct', 0)}%",
    f"8. **Unknown Question:** กฎ \"เกิน threshold ข้อใดข้อหนึ่ง\" ตรงกับ label **{kpi['label_rule_agreement_pct']}%** → `is_high_defect` น่าจะเป็น *label ที่สร้างจากกฎ* ไม่ใช่การนับของเสียจริง ต้องถามเจ้าของข้อมูลก่อนนำไปใช้ตัดสินใจ/ทำโมเดล",
]
for l in INSIGHTS:
    print(l)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Datasets & Widgets

# COMMAND ----------

def ds(name, display, sql):
    return {"name": name, "displayName": display, "queryLines": [sql]}

DATASETS = [
    ds("ds_kpi",     "KPI overview (Delta)", f"SELECT * FROM {G}.kpi_overview"),
    ds("ds_kpi_pg",  "KPI overview (Lakebase PostgreSQL)",
       f"SELECT total_batches, CAST(high_defect_pct AS DOUBLE) AS high_defect_pct FROM {PG}.vw_kpi_overview"),
    ds("ds_impact",  "Driver impact (Controllable vs Outcome)",
       f"SELECT concat(driver, ' (', rule, ')') AS driver_rule, driver_role, gap_pp FROM {G}.mart_driver_impact"),
    ds("ds_curve",   "High-defect % by maintenance hour",
       f"SELECT concat(lpad(CAST(maintenance_hours_per_week AS STRING), 2, '0'), ' h') AS hours, batches, high_defect_pct FROM {G}.mart_maint_hours_curve"),
    ds("ds_volume",  "Production volume bands",
       f"SELECT concat(band_order, '. ', band) AS band_label, batches, high_defect_pct, avg_cost_per_unit FROM {G}.mart_driver_bands WHERE driver = 'Production volume'"),
    ds("ds_defect",  "Defect rate bands (outcome)",
       f"SELECT concat(band_order, '. ', band) AS band_label, batches, high_defect_pct FROM {G}.mart_driver_bands WHERE driver = 'Defect rate'"),
    ds("ds_quality", "Quality score bands (outcome)",
       f"SELECT concat(band_order, '. ', band) AS band_label, batches, high_defect_pct FROM {G}.mart_driver_bands WHERE driver = 'Quality score'"),
    ds("ds_segment_pg", "Controllable risk segment (Lakebase PostgreSQL)",
       f"SELECT CASE risk_segment WHEN 'Low' THEN '1. Low (0 flag)' WHEN 'Medium' THEN '2. Medium (1 flag)' ELSE '3. High (2 flags)' END AS risk_segment, "
       f"batches, CAST(high_defect_pct AS DOUBLE) AS high_defect_pct, CAST(production_cost AS DOUBLE) AS production_cost FROM {PG}.vw_risk_segment"),
    ds("ds_matrix",  "Risk matrix (maintenance × volume)",
       f"SELECT concat(maintenance_band_order, '. ', maintenance_band) AS maintenance_band, concat(volume_band_order, '. ', volume_band) AS volume_band, batches, high_defect_pct FROM {G}.mart_risk_matrix"),
    ds("ds_action",  "Action matrix (controllable segment × QC)",
       f"SELECT concat(risk_segment_order, '. ', risk_segment) AS risk_segment, qc_status, batches, high_defect_pct FROM {G}.mart_action_matrix"),
    ds("ds_label",   "Unknown question — label vs threshold rule",
       f"SELECT outcome, SUM(`count`) AS batches FROM {G}.mart_label_rule GROUP BY outcome"),
]

def q(dataset, fields):
    return [{"name": "main_query", "query": {"datasetName": dataset,
             "fields": [{"name": f, "expression": f"`{f}`"} for f in fields], "disaggregated": True}}]

def counter(name, dataset, field, title, x, y, w_=1, h=2):
    return {"widget": {"name": name, "queries": q(dataset, [field]),
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": field, "displayName": title}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w_, "height": h}}

def bar(name, dataset, xf, yf, title, x, y, w_=3, h=6, xlabel=None, ylabel=None, horizontal=False, color=None):
    enc_cat = {"fieldName": xf, "scale": {"type": "categorical", "sort": {"by": "natural-order"}}, "displayName": xlabel or xf}
    enc_val = {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": ylabel or yf}
    if horizontal:                                        # เรียงแท่งจากมากไปน้อย
        enc_cat["scale"]["sort"] = {"by": "x-reversed"}
    enc = {"x": enc_val, "y": enc_cat} if horizontal else {"x": enc_cat, "y": enc_val}
    fields = [xf, yf]
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
        fields.append(color)
    enc["label"] = {"show": True}
    return {"widget": {"name": name, "queries": q(dataset, fields),
                       "spec": {"version": 3, "widgetType": "bar", "encodings": enc,
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w_, "height": h}}

def heatmap(name, dataset, xf, yf, cf, title, x, y, w_=3, h=7, xlabel=None, ylabel=None):
    return {"widget": {"name": name, "queries": q(dataset, [xf, yf, cf]),
                       "spec": {"version": 3, "widgetType": "heatmap",
                                "encodings": {"x": {"fieldName": xf, "scale": {"type": "categorical", "sort": {"by": "natural-order"}}, "displayName": xlabel or xf},
                                              "y": {"fieldName": yf, "scale": {"type": "categorical", "sort": {"by": "natural-order"}}, "displayName": ylabel or yf},
                                              "color": {"fieldName": cf, "scale": {"type": "quantitative", "colorRamp": {"mode": "scheme", "scheme": "yelloworangered"}}, "displayName": "High-defect %"},
                                              "label": {"show": True}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w_, "height": h}}

def text(name, lines, x, y, w_=6, h=2):
    lines = [l if l.endswith("\n") else l + "\n" for l in lines]   # 1 element = 1 บรรทัด markdown
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
            "position": {"x": x, "y": y, "width": w_, "height": h}}

LAYOUT = [
    text("t_title", ["## Manufacturing Defect — Gold Layer Insights",
                     "Source: Bronze → Silver → **Gold (Delta + Lakebase PostgreSQL)** · target = `is_high_defect` · "
                     "**Controllable** = ปัจจัยที่ทีมปรับได้ · **Outcome** = ตัววัดผลคุณภาพ (ใช้เป็น QC alert)"], 0, 0, 6, 2),
    counter("c_batches",   "ds_kpi_pg", "total_batches",                 "Total batches (PostgreSQL)",       0, 2),
    counter("c_high",      "ds_kpi_pg", "high_defect_pct",               "High-defect % (PostgreSQL)",        1, 2),
    counter("c_costshare", "ds_kpi",    "cost_share_in_high_defect_pct", "Cost in high-defect batches (%)",  2, 2),
    counter("c_lowrisk",   "ds_kpi",    "low_risk_batch_pct",            "No controllable flag (%)",          3, 2),
    counter("c_qc",        "ds_kpi",    "qc_alert_batch_pct",            "QC alert batches (%)",              4, 2),
    counter("c_label",     "ds_kpi",    "label_rule_agreement_pct",      "Label = threshold rule (%)",        5, 2),

    bar("b_impact", "ds_impact", "driver_rule", "gap_pp",
        "Q2/Q7 · Gap of high-defect % at best threshold (pp) — Controllable vs Outcome", 0, 4, 3, 7,
        xlabel="Driver (threshold)", ylabel="Gap (pp)", horizontal=True, color="driver_role"),
    bar("b_segment", "ds_segment_pg", "risk_segment", "high_defect_pct",
        "Q8 · High-defect % by controllable risk segment (from Lakebase PostgreSQL)", 3, 4, 3, 7,
        xlabel="Controllable risk segment", ylabel="High-defect %"),

    bar("b_curve", "ds_curve", "hours", "high_defect_pct", "Q3 · High-defect % by maintenance hours/week (step at threshold)", 0, 11, 3, 6,
        xlabel="Maintenance hours/week", ylabel="High-defect %"),
    bar("b_volume", "ds_volume", "band_label", "high_defect_pct", "Q6 · High-defect % by production volume", 3, 11, 3, 6,
        xlabel="Production volume", ylabel="High-defect %"),
    bar("b_defect", "ds_defect", "band_label", "high_defect_pct", "Q4 · QC monitoring (outcome) · High-defect % by defect rate", 0, 17, 3, 6,
        xlabel="Defect rate", ylabel="High-defect %"),
    bar("b_quality", "ds_quality", "band_label", "high_defect_pct", "Q5 · QC monitoring (outcome) · High-defect % by quality score", 3, 17, 3, 6,
        xlabel="Quality score", ylabel="High-defect %"),

    heatmap("h_matrix", "ds_matrix", "volume_band", "maintenance_band", "high_defect_pct",
            "Controllable risk matrix · maintenance hours × production volume", 0, 23, 3, 7,
            xlabel="Production volume", ylabel="Maintenance hours/week"),
    heatmap("h_action", "ds_action", "qc_status", "risk_segment", "high_defect_pct",
            "Q8 · Operating window · before production (segment) × after production (QC)", 3, 23, 3, 7,
            xlabel="QC status (outcome)", ylabel="Controllable risk segment"),

    bar("b_label", "ds_label", "outcome", "batches",
        "Q9 · Unknown Question — does a simple threshold rule reproduce is_high_defect?", 0, 30, 3, 7,
        xlabel="Rule vs label", ylabel="Batches", horizontal=True),
    text("t_insight", INSIGHTS, 3, 30, 3, 7),
]

SPEC = {"datasets": DATASETS, "pages": [{"name": "overview", "displayName": "Gold Insights", "layout": LAYOUT}]}
print("widgets:", len(LAYOUT), "datasets:", len(DATASETS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create หรือ Update (idempotent) แล้ว publish

# COMMAND ----------

existing = [d for d in w.lakeview.list() if d.display_name == DASH_NAME]

# เก็บหน้าอื่นที่ notebook อื่นเพิ่มไว้ (เช่นหน้า ML จาก ML_Predictive) — แทนที่เฉพาะหน้า overview
if existing:
    cur = json.loads(w.lakeview.get(existing[0].dashboard_id).serialized_dashboard or "{}")
    other_pages = [pg for pg in cur.get("pages", []) if pg.get("name") != "overview"]
    used = {qq["query"]["datasetName"] for pg in other_pages for it in pg.get("layout", [])
            for qq in it.get("widget", {}).get("queries", []) if "query" in qq}
    my_ds = {d["name"] for d in DATASETS}
    extra_ds = [d for d in cur.get("datasets", []) if d["name"] in used and d["name"] not in my_ds]
    SPEC = {"datasets": DATASETS + extra_ds, "pages": SPEC["pages"] + other_pages}
    print("Kept other pages:", [pg.get("displayName") for pg in other_pages])

payload = Dashboard(display_name=DASH_NAME, parent_path=FOLDER, warehouse_id=WAREHOUSE_ID,
                    serialized_dashboard=json.dumps(SPEC))
if existing:
    dash = w.lakeview.update(dashboard_id=existing[0].dashboard_id,
                             dashboard=Dashboard(display_name=DASH_NAME, warehouse_id=WAREHOUSE_ID,
                                                 serialized_dashboard=json.dumps(SPEC)))
    print("Updated dashboard:", dash.dashboard_id)
else:
    dash = w.lakeview.create(dashboard=payload)
    print("Created dashboard:", dash.dashboard_id)

w.lakeview.publish(dashboard_id=dash.dashboard_id, warehouse_id=WAREHOUSE_ID, embed_credentials=True)
host = w.config.host.rstrip("/")
print("Dashboard URL:", f"{host}/dashboardsv3/{dash.dashboard_id}/published")
print("DASHBOARD_ID", dash.dashboard_id)