-- 1) Vérifs rapides avant d’entraîner
--A. Distribution de la cible (important)

SELECT
  is_active_90d,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER(), 4) AS share
FROM `online-retail-project1.ecom_ml.features_customer_snapshot`
GROUP BY is_active_90d;

--B. Valeurs nulles dans les features

SELECT
  COUNTIF(recency_days IS NULL) AS null_recency,
  COUNTIF(total_orders IS NULL) AS null_total_orders,
  COUNTIF(frequency_12m IS NULL) AS null_frequency_12m,
  COUNTIF(monetary_12m IS NULL) AS null_monetary_12m
FROM `online-retail-project1.ecom_ml.features_customer_snapshot`;

-- Séparation 
CREATE OR REPLACE TABLE `online-retail-project1.ecom_ml.ml_input` AS
SELECT
  customer_id,
  is_active_90d,
  recency_days,
  total_orders,
  frequency_12m,
  monetary_12m,
  IF(ABS(MOD(FARM_FINGERPRINT(customer_id), 100)) < 80, 'TRAIN', 'TEST') AS split
FROM `online-retail-project1.ecom_ml.features_customer_snapshot`;




-- Entraîner le modèle BigQuery ML (Logistic Regression)

CREATE OR REPLACE MODEL `online-retail-project1.ecom_ml.model_active_90d_lr_v2`
OPTIONS(
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['is_active_90d'],
  data_split_method = 'AUTO_SPLIT',
  auto_class_weights = TRUE,
  max_iterations = 50
) AS
SELECT
  is_active_90d,
  total_orders,
  frequency_12m,
  monetary_12m
FROM `online-retail-project1.ecom_ml.features_customer_snapshot`;



-- Évaluer le modèle

SELECT *
FROM ML.EVALUATE(
  MODEL `online-retail-project1.ecom_ml.model_active_90d_lr_v2`
);



-- Voir l’importance (poids) des variables

SELECT *
FROM ML.WEIGHTS(
  MODEL `online-retail-project1.ecom_ml.model_active_90d_lr_v2`
)
ORDER BY ABS(weight) DESC;



-- Scoring (probabilité d’être actif)

CREATE OR REPLACE TABLE `online-retail-project1.ecom_ml.scoring_active_90d` AS
SELECT
  customer_id,
  predicted_is_active_90d,
  predicted_is_active_90d_probs[OFFSET(1)].prob AS proba_active_90d
FROM ML.PREDICT(
  MODEL `online-retail-project1.ecom_ml.model_active_90d_lr`,
  (
    SELECT
      customer_id,
      recency_days,
      total_orders,
      frequency_12m,
      monetary_12m
    FROM `online-retail-project1.ecom_ml.features_customer_snapshot`
  )
);
