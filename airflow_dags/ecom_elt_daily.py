from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# =========================
# CONFIG
# =========================
PROJECT_ID = "online-retail-project1"
BUCKET = "gcs-elt-ecom-raw-euw1"

# Fichier "source" existant (on le recopie chaque jour dans ingestion_dt={{ ds }})
SOURCE_OBJECT = "raw/online_retail/ingestion_dt=2025-12-28/online_retail_II.csv"
DEST_OBJECT = "raw/online_retail/ingestion_dt={{ ds }}/online_retail_II.csv"

DEFAULT_ARGS = {"owner": "data", "retries": 1}

# =========================
# SQL QUERIES
# =========================
STG_SQL = """
CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.stg_online_retail` AS
SELECT
  CAST(Invoice AS STRING) AS invoice_no,
  CAST(StockCode AS STRING) AS stock_code,
  NULLIF(TRIM(CAST(Description AS STRING)), '') AS description,
  CAST(Quantity AS INT64) AS quantity,
  CAST(Price AS NUMERIC) AS unit_price,
  SAFE_MULTIPLY(CAST(Quantity AS NUMERIC), CAST(Price AS NUMERIC)) AS line_amount,
  InvoiceDate AS invoice_ts,
  DATE(InvoiceDate) AS invoice_date,
  CAST(CAST(`Customer ID` AS INT64) AS STRING) AS customer_id,
  NULLIF(TRIM(CAST(Country AS STRING)), '') AS country,
  CURRENT_TIMESTAMP() AS load_ts
FROM `online-retail-project1.ecom_raw.raw_online_retail`
WHERE Quantity > 0
  AND Price > 0
  AND InvoiceDate IS NOT NULL
  AND `Customer ID` IS NOT NULL;
"""

FACT_ORDERS_SQL = """
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
"""

FEATURES_SQL = """
CREATE OR REPLACE TABLE `online-retail-project1.ecom_ml.features_customer_snapshot` AS
WITH ref AS (SELECT DATE '2011-12-09' AS snapshot_date),
orders AS (
  SELECT invoice_no, invoice_date, customer_id, order_amount
  FROM `online-retail-project1.ecom_transformed.fact_orders`
),
agg AS (
  SELECT
    customer_id,
    DATE_DIFF((SELECT snapshot_date FROM ref), MAX(invoice_date), DAY) AS recency_days,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(DISTINCT IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), invoice_no, NULL)) AS frequency_12m,
    SUM(IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), order_amount, 0)) AS monetary_12m,
    IF(MAX(invoice_date) >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 90 DAY), 1, 0) AS is_active_90d
  FROM orders
  GROUP BY customer_id
)
SELECT * FROM agg;
"""

# =========================
# DAG
# =========================
with DAG(
    dag_id="ecom_elt_daily",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "elt", "bigquery"],
) as dag:

    # 1) Copier un fichier "source" existant vers le dossier du jour ingestion_dt={{ ds }}
    copy_to_today = GCSToGCSOperator(
        task_id="copy_source_to_today",
        source_bucket=BUCKET,
        source_object=SOURCE_OBJECT,
        destination_bucket=BUCKET,
        destination_object=DEST_OBJECT,
        move_object=False,  # copie (pas move)
    )

    # 2) Charger le fichier du jour depuis GCS vers BigQuery RAW
    load_raw = GCSToBigQueryOperator(
        task_id="load_raw",
        bucket=BUCKET,
        source_objects=[DEST_OBJECT],
        destination_project_dataset_table=f"{PROJECT_ID}.ecom_raw.raw_online_retail",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    # 3) Construire STG (clean minimal + typage + line_amount)
    build_stg = BigQueryInsertJobOperator(
        task_id="build_stg",
        configuration={"query": {"query": STG_SQL, "useLegacySql": False}},
    )

    # 4) Construire FACT_ORDERS (1 ligne = 1 invoice)
    build_fact_orders = BigQueryInsertJobOperator(
        task_id="build_fact_orders",
        configuration={"query": {"query": FACT_ORDERS_SQL, "useLegacySql": False}},
    )

    # 5) Construire les features ML-ready
    build_features = BigQueryInsertJobOperator(
        task_id="build_features",
        configuration={"query": {"query": FEATURES_SQL, "useLegacySql": False}},
    )

    # Orchestration
    copy_to_today >> load_raw >> build_stg >> build_fact_orders >> build_features
