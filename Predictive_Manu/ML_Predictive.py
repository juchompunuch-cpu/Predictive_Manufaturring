# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML_Predictive — ทำนาย high-defect จาก Gold + MLflow
# MAGIC
# MAGIC **Input:** `gold_predictive.batch_360` · **Target:** `is_high_defect`
# MAGIC
# MAGIC | ขั้น | รายละเอียด |
# MAGIC |---|---|
# MAGIC | 1 | แบ่ง train/test 80/20 แบบ stratified (random_state=42) |
# MAGIC | 2 | Baseline: กฎ 4 risk flags (`risk_flag_count >= 1` → high defect) |
# MAGIC | 3 | Logistic Regression (StandardScaler + class_weight="balanced") |
# MAGIC | 4 | Gradient Boosting |
# MAGIC | 5 | วัดผล ROC-AUC, F1, Precision, Recall, Balanced accuracy + confusion matrix → log ลง MLflow |
# MAGIC | 6 | Register โมเดลที่ดีที่สุด (ROC-AUC) ใน Unity Catalog `gold_predictive.defect_model` |
# MAGIC | 7 | เขียน `predictions`, `model_comparison`, `model_feature_importance` และเพิ่มหน้า **Model** ใน dashboard |
# MAGIC
# MAGIC หมายเหตุ: target ไม่สมดุล (84% high defect) จึงไม่ใช้ accuracy อย่างเดียว

# COMMAND ----------

# MAGIC %pip install "mlflow>=2.14" scikit-learn --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import numpy as np
import pandas as pd
import mlflow
from mlflow.models import infer_signature
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (roc_auc_score, f1_score, precision_score, recall_score,
                             balanced_accuracy_score, accuracy_score, confusion_matrix)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ME = w.current_user.me().user_name
C = "predictive_manufacring_defect"
G = f"{C}.gold_predictive"
MODEL_NAME = f"{G}.defect_model"
EXPERIMENT = f"/Users/{ME}/manufacturing_defect_model"

FEATURES = [
    "production_volume", "production_cost", "supplier_quality_pct", "delivery_delay_days",
    "defect_rate", "quality_score_pct", "maintenance_hours_per_week", "downtime_pct",
    "inventory_turnover_ratio", "stockout_rate", "worker_productivity_pct", "safety_incidents_count",
    "energy_consumption_kwh", "energy_efficiency_factor", "additive_process_time_hrs", "additive_material_cost_usd",
]
TARGET = "is_high_defect"

pdf = spark.table(f"{G}.batch_360").select("production_id", "risk_flag_count", "risk_segment", TARGET, *FEATURES).toPandas()
pdf[FEATURES] = pdf[FEATURES].astype("float64")
pdf[TARGET] = pdf[TARGET].astype(int)
print("rows:", len(pdf), "| high-defect %:", round(pdf[TARGET].mean() * 100, 2))

train_df, test_df = train_test_split(pdf, test_size=0.2, stratify=pdf[TARGET], random_state=42)
X_train, y_train = train_df[FEATURES], train_df[TARGET]
X_test, y_test = test_df[FEATURES], test_df[TARGET]
print("train:", len(train_df), "test:", len(test_df))

# COMMAND ----------

def evaluate(y_true, y_pred, y_score):
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return {
        "roc_auc": float(roc_auc_score(y_true, y_score)),
        "f1": float(f1_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp),
    }

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT)

results, models, run_ids, uris = {}, {}, {}, {}

# Baseline: 4 risk flags
with mlflow.start_run(run_name="baseline_4_flags") as run:
    y_pred = (test_df["risk_flag_count"] >= 1).astype(int)
    m = evaluate(y_test, y_pred, test_df["risk_flag_count"])
    mlflow.log_params({"model": "rule: risk_flag_count >= 1",
                       "flags": "maint>10, volume>800, defect_rate>3, quality<75"})
    mlflow.log_metrics({k: v for k, v in m.items()})
    results["Baseline (4 flags)"] = m; run_ids["Baseline (4 flags)"] = run.info.run_id

CANDIDATES = {
    "Logistic Regression": Pipeline([("scaler", StandardScaler()),
                                     ("clf", LogisticRegression(class_weight="balanced", max_iter=2000, random_state=42))]),
    "Gradient Boosting": GradientBoostingClassifier(n_estimators=200, learning_rate=0.05, max_depth=3, random_state=42),
}
for name, model in CANDIDATES.items():
    with mlflow.start_run(run_name=name.lower().replace(" ", "_")) as run:
        model.fit(X_train, y_train)
        proba = model.predict_proba(X_test)[:, 1]
        pred = (proba >= 0.5).astype(int)
        m = evaluate(y_test, pred, proba)
        mlflow.log_params({"model": name, "features": len(FEATURES), "test_size": 0.2, "random_state": 42,
                           **({"n_estimators": 200, "learning_rate": 0.05, "max_depth": 3} if "Boost" in name
                              else {"class_weight": "balanced", "max_iter": 2000})})
        mlflow.log_metrics(m)
        sig = infer_signature(X_train.head(50), model.predict_proba(X_train.head(50))[:, 1])
        info = mlflow.sklearn.log_model(model, artifact_path="model", signature=sig, input_example=X_train.head(5),
                                        serialization_format="cloudpickle")
        results[name] = m; models[name] = model; run_ids[name] = run.info.run_id; uris[name] = info.model_uri

cmp = pd.DataFrame(results).T.reset_index().rename(columns={"index": "model"})
display(cmp[["model", "roc_auc", "f1", "precision", "recall", "balanced_accuracy", "accuracy", "tn", "fp", "fn", "tp"]].round(4))

# COMMAND ----------

# เลือกโมเดลที่ดีที่สุดตาม ROC-AUC (เฉพาะโมเดล ML) แล้ว register ใน Unity Catalog
best = max(models, key=lambda n: results[n]["roc_auc"])
print("Best model:", best, "ROC-AUC =", round(results[best]["roc_auc"], 4))
reg = mlflow.register_model(uris[best], MODEL_NAME)
print("Registered:", MODEL_NAME, "version", reg.version)
try:
    from mlflow import MlflowClient
    MlflowClient().set_registered_model_alias(MODEL_NAME, "champion", reg.version)
    print("Alias champion ->", reg.version)
except Exception as e:
    print("alias skipped:", str(e)[:200])

# COMMAND ----------

# Feature importance
if best == "Gradient Boosting":
    imp = models[best].feature_importances_
else:
    imp = np.abs(models[best].named_steps["clf"].coef_[0])
fi = pd.DataFrame({"feature": FEATURES, "importance": imp}).sort_values("importance", ascending=False)
fi["model"] = best
gb_imp = pd.DataFrame({"feature": FEATURES, "importance": models["Gradient Boosting"].feature_importances_, "model": "Gradient Boosting"})
lr_imp = pd.DataFrame({"feature": FEATURES, "importance": np.abs(models["Logistic Regression"].named_steps["clf"].coef_[0]), "model": "Logistic Regression"})
fi_all = pd.concat([gb_imp, lr_imp]).sort_values(["model", "importance"], ascending=[True, False])
display(fi_all)

# Predictions ทุกแถว (บอกด้วยว่าอยู่ใน train หรือ test)
pdf["split"] = np.where(pdf["production_id"].isin(test_df["production_id"]), "test", "train")
pdf["prob_high_defect"] = models[best].predict_proba(pdf[FEATURES])[:, 1]
pdf["pred_high_defect"] = (pdf["prob_high_defect"] >= 0.5).astype(int)
pdf["baseline_pred"] = (pdf["risk_flag_count"] >= 1).astype(int)
pdf["model_name"] = best
pdf["model_version"] = int(reg.version)

pred_sdf = spark.createDataFrame(pdf[["production_id", "split", "is_high_defect", "prob_high_defect", "pred_high_defect",
                                      "baseline_pred", "risk_flag_count", "risk_segment", "model_name", "model_version"]])
pred_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.predictions")

cmp["is_best"] = cmp["model"] == best
cmp["mlflow_run_id"] = cmp["model"].map(run_ids)
spark.createDataFrame(cmp).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.model_comparison")
spark.createDataFrame(fi_all).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.model_feature_importance")
print("Tables written: predictions, model_comparison, model_feature_importance")
display(spark.sql(f"""
SELECT risk_segment, COUNT(*) batches, ROUND(AVG(is_high_defect)*100,2) actual_high_pct,
       ROUND(AVG(prob_high_defect)*100,2) avg_pred_prob_pct
FROM {G}.predictions WHERE split='test' GROUP BY risk_segment ORDER BY risk_segment"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## เพิ่มหน้า "Model" ใน AI/BI Dashboard (idempotent — แทนที่หน้าเดิมถ้ามี)

# COMMAND ----------

from databricks.sdk.service.dashboards import Dashboard
DASH_NAME = "Manufacturing Defect - Gold Insights"
dash = next(d for d in w.lakeview.list() if d.display_name == DASH_NAME)
full = w.lakeview.get(dash.dashboard_id)
spec = json.loads(full.serialized_dashboard)
WAREHOUSE_ID = full.warehouse_id or next(x for x in w.warehouses.list()).id

def ds(name, display, sql): return {"name": name, "displayName": display, "queryLines": [sql]}
def q(dataset, fields):
    return [{"name": "main_query", "query": {"datasetName": dataset,
             "fields": [{"name": f, "expression": f"`{f}`"} for f in fields], "disaggregated": True}}]
def counter(name, dataset, field, title, x, y, w_=2, h=2):
    return {"widget": {"name": name, "queries": q(dataset, [field]),
            "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": field, "displayName": title}},
                     "frame": {"showTitle": True, "title": title}}}, "position": {"x": x, "y": y, "width": w_, "height": h}}
def bar(name, dataset, xf, yf, title, x, y, w_=3, h=6, horizontal=False, color=None, sort_value=False):
    cat = {"fieldName": xf, "scale": {"type": "categorical", "sort": {"by": ("x-reversed" if horizontal else "y-reversed") if sort_value else "natural-order"}}, "displayName": xf}
    val = {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": yf}
    enc = {"x": val, "y": cat} if horizontal else {"x": cat, "y": val}
    fields = [xf, yf]
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}; fields.append(color)
    enc["label"] = {"show": True}
    return {"widget": {"name": name, "queries": q(dataset, fields),
            "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w_, "height": h}}
def text(name, lines, x, y, w_=6, h=2):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [l + "\n" for l in lines]}},
            "position": {"x": x, "y": y, "width": w_, "height": h}}

M_DATASETS = [
    ds("m_best", "Best model", f"SELECT model, ROUND(roc_auc,4) AS roc_auc, ROUND(f1,4) AS f1, ROUND(recall,4) AS recall FROM {G}.model_comparison WHERE is_best"),
    ds("m_cmp", "Model comparison", f"SELECT model, metric, ROUND(value,4) AS value FROM {G}.model_comparison "
        "UNPIVOT (value FOR metric IN (roc_auc, f1, precision, recall, balanced_accuracy))"),
    ds("m_fi", "Feature importance (best)", f"SELECT feature, ROUND(importance,4) AS importance FROM {G}.model_feature_importance "
        f"WHERE model = (SELECT model FROM {G}.model_comparison WHERE is_best) ORDER BY importance DESC LIMIT 10"),
    ds("m_seg", "Predicted vs actual by segment (test)", f"SELECT risk_segment, measure, pct FROM (SELECT risk_segment, "
        f"ROUND(AVG(is_high_defect)*100,2) AS actual_high_pct, ROUND(AVG(prob_high_defect)*100,2) AS predicted_prob_pct "
        f"FROM {G}.predictions WHERE split='test' GROUP BY risk_segment) UNPIVOT (pct FOR measure IN (actual_high_pct, predicted_prob_pct))"),
]
b = results[best]; base = results["Baseline (4 flags)"]
M_LAYOUT = [
    text("m_title", ["## Predictive model — high-defect",
                     f"Best: **{best}** (UC `{MODEL_NAME}` v{reg.version}) · test set {len(test_df)} batches · MLflow experiment `{EXPERIMENT}`"], 0, 0, 6, 2),
    counter("m_c_auc", "m_best", "roc_auc", "Best ROC-AUC", 0, 2),
    counter("m_c_f1", "m_best", "f1", "Best F1", 2, 2),
    counter("m_c_rec", "m_best", "recall", "Best recall", 4, 2),
    bar("m_b_cmp", "m_cmp", "metric", "value", "Model vs baseline (4 flags) on test set", 0, 4, 6, 7, color="model"),
    bar("m_b_fi", "m_fi", "feature", "importance", "Top 10 features (best model)", 0, 11, 3, 7, horizontal=True, sort_value=True),
    bar("m_b_seg", "m_seg", "risk_segment", "pct", "Actual high-defect % vs predicted probability by risk segment (test)", 3, 11, 3, 7, color="measure"),
    text("m_note", [
        "### อ่านผล",
        f"- Baseline 4 flags: ROC-AUC {base['roc_auc']:.3f}, F1 {base['f1']:.3f}, recall {base['recall']:.3f}",
        f"- {best}: ROC-AUC {b['roc_auc']:.3f}, F1 {b['f1']:.3f}, recall {b['recall']:.3f}",
        "- Target ไม่สมดุล (84% high) ดู ROC-AUC / balanced accuracy ประกอบ F1",
    ], 0, 18, 6, 3),
]
spec["datasets"] = [d for d in spec["datasets"] if not d["name"].startswith("m_")] + M_DATASETS
spec["pages"] = [p for p in spec["pages"] if p["name"] != "model"] + [{"name": "model", "displayName": "Model", "layout": M_LAYOUT}]
w.lakeview.update(dashboard_id=dash.dashboard_id, dashboard=Dashboard(display_name=DASH_NAME, warehouse_id=WAREHOUSE_ID,
                                                                         serialized_dashboard=json.dumps(spec)))
w.lakeview.publish(dashboard_id=dash.dashboard_id, warehouse_id=WAREHOUSE_ID, embed_credentials=True)
print("Dashboard updated with page 'Model'")

# COMMAND ----------

# ทดลองเพิ่ม: ใช้เฉพาะตัวแปรที่รู้ก่อนผลิต (ตัด Outcome ออก) เพื่อดู label leakage
OUTCOME = ["defect_rate", "quality_score_pct"]
PRE_FEATURES = [f for f in FEATURES if f not in OUTCOME]   # 14 ตัวแปร

gb_pre = GradientBoostingClassifier(n_estimators=200, learning_rate=0.05, max_depth=3, random_state=42)
gb_pre.fit(train_df[PRE_FEATURES], y_train)

proba_pre = gb_pre.predict_proba(test_df[PRE_FEATURES])[:, 1]
pred_pre = (proba_pre >= 0.5).astype(int)
m_pre = evaluate(y_test, pred_pre, proba_pre)

with mlflow.start_run(run_name="gb_pre_production_only"):
    mlflow.log_params({"model": "Gradient Boosting", "features": len(PRE_FEATURES), "excluded": ",".join(OUTCOME)})
    mlflow.log_metrics(m_pre)

print("GB (pre-production features only)")
print(f"  ROC-AUC {m_pre['roc_auc']:.4f}  F1 {m_pre['f1']:.4f}  BalAcc {m_pre['balanced_accuracy']:.4f}  "
      f"CM tn={m_pre['tn']} fp={m_pre['fp']} fn={m_pre['fn']} tp={m_pre['tp']}")

# COMMAND ----------

print("ML_SUMMARY")
for k, v in results.items():
    print(f"  {k:<22} ROC-AUC {v['roc_auc']:.4f}  F1 {v['f1']:.4f}  P {v['precision']:.4f}  R {v['recall']:.4f}  BalAcc {v['balanced_accuracy']:.4f}  CM tn={v['tn']} fp={v['fp']} fn={v['fn']} tp={v['tp']}")
print("  best:", best, "| UC model:", MODEL_NAME, "v", reg.version)
print("  top features:", ", ".join(fi["feature"].head(5)))