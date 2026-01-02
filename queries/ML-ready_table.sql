SELECT MAX(invoice_date)
FROM `online-retail-project1.ecom_transformed.stg_online_retail`;

-- Création de la table ML-ready

CREATE OR REPLACE TABLE `online-retail-project1.ecom_ml.features_customer_snapshot` AS
WITH ref AS (
  SELECT DATE '2011-12-09' AS snapshot_date
),

orders AS (
  SELECT
    invoice_no,
    invoice_date,
    customer_id,
    order_amount
  FROM `online-retail-project1.ecom_transformed.fact_orders`
),

agg AS (
  SELECT
    customer_id,

    -- RFM
    DATE_DIFF((SELECT snapshot_date FROM ref), MAX(invoice_date), DAY) AS recency_days,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(DISTINCT IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), invoice_no, NULL)) AS frequency_12m,
    SUM(IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), order_amount, 0)) AS monetary_12m,

    -- Montants
    SUM(order_amount) AS lifetime_revenue,
    SAFE_DIVIDE(
      SUM(IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), order_amount, 0)),
      COUNT(DISTINCT IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), invoice_no, NULL))
    ) AS avg_order_value_12m,

    -- Temporalité
    DATE_DIFF(MAX(invoice_date), MIN(invoice_date), DAY) AS customer_lifetime_days,
    COUNT(DISTINCT EXTRACT(YEAR FROM invoice_date) * 100 + EXTRACT(MONTH FROM invoice_date)) AS active_months,

    -- Cible ML (classification simple)
    IF(
      MAX(invoice_date) >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 90 DAY),
      1,
      0
    ) AS is_active_90d

  FROM orders
  GROUP BY customer_id
)

SELECT *
FROM agg;

-- Vérifications essentielles (à faire)
SELECT
  COUNT(*) AS nb_customers,
  MIN(recency_days) AS min_recency,
  MAX(recency_days) AS max_recency,
  AVG(monetary_12m) AS avg_monetary,
  AVG(is_active_90d) AS active_rate
FROM `online-retail-project1.ecom_ml.features_customer_snapshot`;


SELECT *
FROM `online-retail-project1.ecom_ml.features_customer_snapshot`
LIMIT 10;





