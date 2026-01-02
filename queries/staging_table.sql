CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.stg_online_retail` AS
SELECT
  -- Identifiants
  CAST(Invoice AS STRING) AS invoice_no,
  CAST(StockCode AS STRING) AS stock_code,
  NULLIF(TRIM(CAST(Description AS STRING)), '') AS description,

  -- Mesures
  CAST(Quantity AS INT64) AS quantity,
  CAST(Price AS NUMERIC) AS unit_price,
  SAFE_MULTIPLY(CAST(Quantity AS NUMERIC), CAST(Price AS NUMERIC)) AS line_amount,

  -- Dates
  CAST(InvoiceDate AS TIMESTAMP) AS invoice_ts,
  DATE(InvoiceDate) AS invoice_date,

  -- Client & pays
  CAST(CAST(`Customer ID` AS INT64) AS STRING) AS customer_id,
  NULLIF(TRIM(CAST(Country AS STRING)), '') AS country,

  -- Métadonnées
  CURRENT_TIMESTAMP() AS load_ts
FROM `online-retail-project1.ecom_raw.raw_online_retail`
WHERE 1=1
  AND Quantity IS NOT NULL
  AND Price IS NOT NULL
  AND InvoiceDate IS NOT NULL
  AND Quantity > 0
  AND Price > 0
  AND `Customer ID` IS NOT NULL;


-- Contrôles qualité (après création)

SELECT
  COUNT(*) AS rowss,
  COUNT(DISTINCT invoice_no) AS invoices,
  COUNT(DISTINCT customer_id) AS customers,
  MIN(invoice_date) AS min_date,
  MAX(invoice_date) AS max_date,
  SUM(line_amount) AS revenue
FROM `online-retail-project1.ecom_transformed.stg_online_retail`;

-- vérifie qu’il n’y a plus de valeurs invalides :

SELECT
  COUNTIF(quantity <= 0) AS bad_qty,
  COUNTIF(unit_price <= 0) AS bad_price,
  COUNTIF(invoice_ts IS NULL) AS bad_ts,
  COUNTIF(customer_id IS NULL) AS bad_customer
FROM `online-retail-project1.ecom_transformed.stg_online_retail`;


