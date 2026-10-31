-- Ad-hoc analysis examples on the Gold layer
-- Source: Manufacturing Defect Pipeline report, section 8.6

-- 1) Batches that pass QC (defect rate <= 3, quality score >= 75):
--    how does high-defect % change by maintenance band?
SELECT maintenance_band,
       COUNT(*)                            AS batches,
       ROUND(AVG(is_high_defect) * 100, 2) AS high_defect_pct
FROM manufacturing_gold_pg.gold.vw_batch_360
WHERE defect_rate <= 3
  AND quality_score_pct >= 75
GROUP BY maintenance_band
ORDER BY maintenance_band;

-- 2) Highest-risk batches (watchlist)
SELECT production_id,
       maintenance_hours_per_week,
       defect_rate,
       quality_score_pct,
       production_volume
FROM predictive_manufacring_defect.gold_predictive.batch_360
WHERE risk_flag_count >= 3
ORDER BY risk_flag_count DESC, defect_rate DESC
LIMIT 20;

-- 3) Unknown Question: how often does the 4-rule threshold match the label?
SELECT *
FROM manufacturing_gold_pg.gold.vw_label_rule_check
ORDER BY label_rule_pred, is_high_defect;
