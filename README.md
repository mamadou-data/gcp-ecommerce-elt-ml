# 📊 GCP E-commerce ELT & Machine Learning Pipeline

## 🧠 Présentation du projet

Ce projet met en œuvre un **pipeline ELT complet et automatisé sur Google Cloud Platform**, depuis l’ingestion de données brutes jusqu’à l’entraînement d’un **modèle de Machine Learning**, orchestré avec **Cloud Composer (Airflow)**.

🎯 **Objectifs**
- Construire une architecture data **cloud-native et scalable**
- Appliquer les bonnes pratiques **ELT (Extract – Load – Transform)**
- Produire des **tables analytiques fiables**
- Créer une **table ML-ready**
- Entraîner et évaluer un **modèle de classification client**
- Démontrer une démarche **professionnelle et reproductible**

---

## 🏗️ Architecture globale

```
Kaggle CSV
↓
Google Cloud Storage (RAW)
↓
BigQuery RAW
↓
BigQuery TRANSFORMED (STG, FACT)
↓
BigQuery ML (features + model)
↓
Airflow (Cloud Composer – orchestration quotidienne)
```

---

## 📂 Dataset

- **Source** : Kaggle – *[Online Retail II](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)*
- **Type** : Transactions e-commerce
- **Période** : 2009 – 2011
- **Données** : factures, produits, clients, quantités, montants

---

## ☁️ Stack technique

- **Google Cloud Storage (GCS)** : stockage des données brutes
- **BigQuery** : moteur analytique & transformations ELT
- **Cloud Composer (Airflow)** : orchestration
- **BigQuery ML** : Machine Learning
- **Python** : DAG Airflow
- **SQL** : transformations & modélisation

---

## 📁 Structure du projet
```
gcp-ecommerce-elt-ml/
│
├── airflow_dags/
│ └── ecom_elt_daily.py
│
├── queries/
│ ├── staging_table.sql
│ ├── modelisation_sql.sql
│ ├── ML-ready-table.sql
│ └── bigquery_ml.sql
│
├── kaggle/
│
├── README.md
└── .gitignore
```

---

## 1️⃣ Ingestion des données (RAW)

### 📥 Téléchargement depuis Kaggle
```bash
kaggle datasets download -d mashlyn/online-retail-ii-uci
unzip online-retail-ii-uci.zip
```
📤 Upload vers Google Cloud Storage
```
gsutil cp online_retail_II.csv \
gs://gcs-elt-ecom-raw-euw1/raw/online_retail/ingestion_dt=2025-12-28/
```
## 2️⃣ Chargement BigQuery (RAW)

Les données sont chargées depuis GCS vers BigQuery avec autodétection du schéma :

Dataset : ecom_raw

Table : raw_online_retail

Format : CSV

## 3️⃣ Transformation ELT – STAGING
🎯 **Objectifs**

Nettoyage minimal

Typage des colonnes

Suppression des valeurs incohérentes

Calcul des montants de ligne

```
CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.stg_online_retail` AS
SELECT
  CAST(Invoice AS STRING) AS invoice_no,
  CAST(StockCode AS STRING) AS stock_code,
  CAST(Quantity AS INT64) AS quantity,
  CAST(Price AS NUMERIC) AS unit_price,
  SAFE_MULTIPLY(CAST(Quantity AS NUMERIC), CAST(Price AS NUMERIC)) AS line_amount,
  DATE(InvoiceDate) AS invoice_date,
  CAST(CAST(`Customer ID` AS INT64) AS STRING) AS customer_id,
  Country
FROM `online-retail-project1.ecom_raw.raw_online_retail`
WHERE Quantity > 0
  AND Price > 0
  AND `Customer ID` IS NOT NULL;
```

## 4️⃣ Table de faits – FACT_ORDERS
```
CREATE OR REPLACE TABLE `online-retail-project1.ecom_transformed.fact_orders` AS
SELECT
  invoice_no,
  invoice_date,
  customer_id,
  COUNT(*) AS total_items,
  SUM(line_amount) AS order_amount
FROM `online-retail-project1.ecom_transformed.stg_online_retail`
GROUP BY invoice_no, invoice_date, customer_id;

```
## 5️⃣ Feature Engineering – Table ML-ready

🎯 **Objectif**

Créer une table 1 ligne = 1 client pour le Machine Learning.
```
CREATE OR REPLACE TABLE `online-retail-project1.ecom_ml.features_customer_snapshot` AS
WITH ref AS (SELECT DATE '2011-12-09' AS snapshot_date)
SELECT
  customer_id,
  DATE_DIFF((SELECT snapshot_date FROM ref), MAX(invoice_date), DAY) AS recency_days,
  COUNT(DISTINCT invoice_no) AS total_orders,
  COUNT(DISTINCT IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), invoice_no, NULL)) AS frequency_12m,
  SUM(IF(invoice_date >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 12 MONTH), order_amount, 0)) AS monetary_12m,
  IF(MAX(invoice_date) >= DATE_SUB((SELECT snapshot_date FROM ref), INTERVAL 90 DAY), 1, 0) AS is_active_90d
FROM `online-retail-project1.ecom_transformed.fact_orders`
GROUP BY customer_id;
```
## 6️⃣ Orchestration avec Airflow (Cloud Composer)

📦 Déploiement du DAG
```
gsutil cp ecom_elt_daily.py \
gs://us-central1-online-retail-c-cbeefab9-bucket/dags/
```

⏱️ **Pipeline quotidien**

Le DAG exécute automatiquement :

* 1. Copie du fichier source vers ingestion_dt={{ ds }}

* 2. Chargement GCS → BigQuery RAW

* 3. Recalcul STG

* 4. Recalcul FACT_ORDERS

* 5. Recalcul des features ML

✅ **DAG** exécuté quotidiennement avec succès

## 7️⃣ Machine Learning avec BigQuery ML

🎯 **Problématique**

Prédire si un client sera actif dans les 90 prochains jours

🧪 Modèle final (sans data leakage)
```
CREATE OR REPLACE MODEL `online-retail-project1.ecom_ml.model_active_90d_lr_v2`
OPTIONS(
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['is_active_90d'],
  data_split_method = 'AUTO_SPLIT',
  auto_class_weights = TRUE
) AS
SELECT
  is_active_90d,
  total_orders,
  frequency_12m,
  monetary_12m
FROM `online-retail-project1.ecom_ml.features_customer_snapshot`;
```

📈 **Résultats du modèle**

| Métrique  | Valeur   |
| --------- | -------- |
| ROC AUC   | **0.88** |
| Accuracy  | 0.78     |
| Precision | 0.82     |
| Recall    | 0.72     |
| F1-score  | 0.77     |

✔ Modèle réaliste

✔ Pas de fuite d’information

✔ Exploitable métier

---

🧠 **Points forts du projet**

- Architecture ELT cloud-native

- Orchestration Airflow en production

- SQL analytique structuré

- Détection et correction d’un data leakage

- Pipeline automatisé et reproductible

- ML intégré directement dans BigQuery

🔜 **Améliorations possibles**

- Boosted Trees (BigQuery ML)

- Ajout de nouvelles features comportementales

- Alerting Airflow

- Dashboard Power BI / Looker

## 👤 Auteur

Mamadou DIEDHIOU

Data Analyst / Chargé d’études statistiques
