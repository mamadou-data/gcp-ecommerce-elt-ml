--- 1) Créer les dimensions (ecom_transformed)
--dim_customer

CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.dim_customer` AS
SELECT
  customer_id,
  MIN(invoice_date) AS first_purchase_date,
  MAX(invoice_date) AS last_purchase_date,
  COUNT(DISTINCT invoice_no) AS lifetime_invoices,
  SUM(line_amount) AS lifetime_revenue,
  ANY_VALUE(country) AS country -- pays le plus fréquent serait mieux, mais ça suffit au départ
FROM `online-retail-project1.ecom_transformed.stg_online_retail`
GROUP BY customer_id;

--dim_product

CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.dim_product` AS
SELECT
  stock_code,
  ANY_VALUE(description) AS description
FROM `online-retail-project1.ecom_transformed.stg_online_retail`
GROUP BY stock_code;

--dim_date 

CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.dim_date` AS
WITH d AS (
  SELECT MIN(invoice_date) AS min_d, MAX(invoice_date) AS max_d
  FROM `online-retail-project1.ecom_transformed.stg_online_retail`
)
SELECT
  day AS date,
  EXTRACT(YEAR FROM day) AS year,
  EXTRACT(MONTH FROM day) AS month,
  EXTRACT(DAY FROM day) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM day) AS day_of_week,
  FORMAT_DATE('%A', day) AS day_name,
  FORMAT_DATE('%B', day) AS month_name,
  EXTRACT(ISOWEEK FROM day) AS iso_week,
  IF(EXTRACT(DAYOFWEEK FROM day) IN (1,7), TRUE, FALSE) AS is_weekend
FROM d,
UNNEST(GENERATE_DATE_ARRAY(min_d, max_d)) AS day;


--- 2) Créer les faits (ecom_transformed)

 -- A) fact_order_items (grain = 1 ligne produit sur une facture)
CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.fact_order_items` AS
SELECT
  invoice_no,
  invoice_date,
  invoice_ts,
  customer_id,
  country,
  stock_code,
  quantity,
  unit_price,
  line_amount
FROM `online-retail-project1.ecom_transformed.stg_online_retail`;


 -- B) fact_orders (grain = 1 facture)
CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.fact_orders` AS
SELECT
  invoice_no,
  invoice_date,
  customer_id,
  country,
  COUNT(DISTINCT stock_code) AS distinct_products,
  SUM(quantity) AS total_items,
  SUM(line_amount) AS order_amount
FROM `online-retail-project1.ecom_transformed.stg_online_retail`
GROUP BY invoice_no, invoice_date, customer_id, country;

--- 3) Créer les vues analytiques (ecom_analytics)
--- A) vw_sales_daily
CREATE OR REPLACE VIEW `online-retail-project1.ecom_analytics.vw_sales_daily` AS
SELECT
  invoice_date AS date,
  COUNT(DISTINCT invoice_no) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(order_amount) AS revenue,
  SAFE_DIVIDE(SUM(order_amount), COUNT(DISTINCT invoice_no)) AS aov
FROM `online-retail-project1.ecom_transformed.fact_orders`
GROUP BY date;

--- B) vw_top_products

CREATE OR REPLACE VIEW `online-retail-project1.ecom_analytics.vw_top_products` AS
SELECT
  f.stock_code,
  p.description,
  SUM(f.quantity) AS units_sold,
  SUM(f.line_amount) AS revenue
FROM `online-retail-project1.ecom_transformed.fact_order_items` f
LEFT JOIN `online-retail-project1.ecom_transformed.dim_product` p
  ON f.stock_code = p.stock_code
GROUP BY
  f.stock_code,
  p.description;




