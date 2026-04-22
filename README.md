# dbt-Snowflake-ELT-Pipeline

An end-to-end ELT pipeline built on Snowflake, dbt, Apache Airflow, and Docker. Uses the TPC-H benchmark dataset (`tpch_sf1`) available natively in Snowflake as the data source. Data is loaded directly into Snowflake, transformed through a layered dbt model architecture (staging → marts → fact tables), tested with generic and singular data quality tests, and orchestrated daily via an Airflow DAG — all containerised with Docker Compose.

> **Note:** Remember to set up your Snowflake account and configure credentials according to your environment before running. See [Local Setup](#local-setup) below.

---

## Architecture Overview

```
Snowflake Sample Data (tpch_sf1)
    orders table · lineitem table
          |
          | dbt source() references
          v
  Snowflake — DBT_DATABASE.DBT_SCHEMA
          |
     [ STAGING LAYER ]  — materialised as Views
          |
     staging_tpch_orders   stg_lineitem
     (renamed + typed)     (MD5 surrogate key + renamed)
          |
          | dbt ref() + macro: discounted_amount()
          v
     [ MARTS LAYER ]  — materialised as Tables
          |
     order_items_mart      order_summary_mart
     (orders x lineitem    (aggregated discount
      join + discount)      + sales by order)
          |
          v
     [ FACT LAYER ]  — materialised as Table
          |
         fact_orders
     (orders x order_summary join · final analytical table)
          |
     Generic + Singular Tests
     (unique · not_null · relationships · accepted_values · date validation · discount validation)
          |
          v
   Airflow DAG — tpch_data_pipeline
   (daily schedule · dependency-chained BashOperator tasks · dbt run + dbt test)
          |
   Docker Compose
   (airflow-webserver · airflow-scheduler · postgres metadata DB)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Data Warehouse | Snowflake (X-Small warehouse) |
| Transformation | dbt Core 1.10.15 + dbt-snowflake 1.10.4 |
| Orchestration | Apache Airflow 2.7.0 |
| Containerisation | Docker Compose |
| Metadata DB | PostgreSQL 13 (Airflow backend) |
| Language | SQL · YAML · Python (DAG) |
| Dataset | Snowflake Sample Data · tpch_sf1 |

---

## Dataset

TPC-H (`tpch_sf1`) is a standard decision-support benchmark dataset available natively in Snowflake under `snowflake_sample_data.tpch_sf1`. Two tables are used:

| Table | Key Fields |
|---|---|
| `orders` | o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate |
| `lineitem` | l_orderkey, l_partkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax |

---

## Repository Structure

```
dbt-Snowflake-ELT-Pipeline/
├── DAG/
│   └── dbt_airflow_pipeline.py       # Airflow DAG — orchestrates all dbt tasks
├── dbt_pipeline/
│   ├── macros/
│   │   └── pricing.sql               # discounted_amount() macro
│   ├── models/
│   │   ├── staging/
│   │   │   ├── tpch_source.yml       # Source definitions + source tests
│   │   │   ├── staging_tpch_orders.sql  # Orders staging view
│   │   │   └── stg_lineitem.sql      # Lineitem staging view + MD5 surrogate key
│   │   └── marts/
│   │       ├── order_items_mart.sql  # Orders x lineitem join + discount calc
│   │       ├── order_summary_mart.sql # Aggregated sales + discount by order
│   │       ├── fact_orders.sql       # Final fact table
│   │       └── generic_test.yml      # Generic dbt tests (unique, not_null, relationships, accepted_values)
│   ├── tests/
│   │   ├── singular_test.sql         # Discount sanity check (amount must be <= 0)
│   │   └── test_valid_date.sql       # Order date range validation (1990–today)
│   └── dbt_project.yml               # dbt project config
├── docker-compose.yml                # Airflow + Postgres services
├── ELT airflow orchestration.png     # DAG screenshot
├── .gitignore
└── README.md
```

---

## dbt Model Architecture

### Staging Layer — materialised as Views

**`staging_tpch_orders`**  
Reads directly from `source('tpch', 'orders')`. Renames all raw fields to clean names: `o_orderkey → order_key`, `o_custkey → customer_key`, `o_orderstatus → order_status`, `o_totalprice → total_price`, `o_orderdate → order_date`.

**`stg_lineitem`**  
Reads from `source('tpch', 'lineitem')`. Generates a surrogate key using `MD5(CONCAT(l_orderkey, '|', l_linenumber))`. Renames and exposes: `order_key`, `part_key`, `line_number`, `quantity`, `extended_price`, `discount_percentage`, `tax_rate`.

---

### Marts Layer — materialised as Tables

**`order_items_mart`**  
Joins `staging_tpch_orders` and `stg_lineitem` on `order_key`. Applies the `discounted_amount()` macro to calculate `item_discount_amount` per line item. Output fields: `item_surrogate_key`, `part_key`, `line_number`, `extended_price`, `order_key`, `customer_key`, `order_date`, `item_discount_amount`.

**`order_summary_mart`**  
Aggregates `order_items_mart` by `order_key`. Produces `gross_item_sales_amount` (SUM of extended_price) and `item_discount_amount` (SUM of discounts) per order.

---

### Fact Layer — materialised as Table

**`fact_orders`**  
Final analytical table. Joins `staging_tpch_orders` with `order_summary_mart` on `order_key`. Combines order-level attributes (total_price, order_date, customer_key) with aggregated item summary metrics. Primary analytical output consumed by downstream BI tools.

---

## Macro

**`discounted_amount(extended_price, discount_percentage, scale=2)`** — defined in `macros/pricing.sql`

Calculates the discount value as a negative decimal:
```sql
(extended_price * discount_percentage * (-1))::numeric(16, scale)
```

Used in `order_items_mart` to compute `item_discount_amount` per line item. Scale defaults to 2 decimal places.

---

## Data Quality Tests

### Generic Tests — `generic_test.yml`

Applied to `fact_orders`:

| Column | Tests |
|---|---|
| `order_key` | unique, not_null, relationships to `staging_tpch_orders.order_key` (severity: warn) |
| `status_code` | accepted_values: `['P', 'O', 'F']` |

Applied to source tables in `tpch_source.yml`:

| Table | Column | Tests |
|---|---|---|
| `orders` | `o_orderkey` | unique, not_null |
| `lineitem` | `l_orderkey` | relationships to `orders.o_orderkey` |

### Singular Tests — `tests/`

**`singular_test.sql`** — checks that no `item_discount_amount` in `fact_orders` is positive (discounts must be zero or negative):
```sql
SELECT * FROM {{ ref('fact_orders') }} WHERE item_discount_amount > 0
```

**`test_valid_date.sql`** — checks that no `order_date` in `fact_orders` is in the future or before 1990-01-01:
```sql
SELECT * FROM {{ ref('fact_orders') }} WHERE date(order_date) > CURRENT_DATE()
   OR date(order_date) < date('1990-01-01')
```

Both singular tests return rows on failure — dbt fails the test if any rows are returned.

---

## Airflow DAG

**DAG ID:** `tpch_data_pipeline`  
**Schedule:** `@daily`  
**Executor:** LocalExecutor  
**Retry:** 1 retry · 5 minute delay

All tasks are `BashOperator` running dbt commands inside the container at `/opt/airflow/dbt`.

### Task Dependency Graph

```
staging_test (dbt test staging.*)
       |
  _____|_____
  |         |
stg_orders  stg_lineitem
  |_________|
       |
  order_items (order_items_mart)
       |
  order_summary (order_summary_mart)
       |
  _____|_____
  |         |
fact_orders (fact_orders)
       |
  _____|___________
  |         |     |
mart_test  singular_test  valid_date
```

Staging source tests run first to validate raw data before any transformation. Mart tests and singular tests run last after the fact table is built.

---

## Docker Compose Setup

Three services:

| Service | Image | Role |
|---|---|---|
| `postgres` | postgres:13 | Airflow metadata database |
| `airflow-webserver` | apache/airflow:2.7.0-python3.10 | Airflow UI on port 8080 · installs dbt on startup |
| `airflow-scheduler` | apache/airflow:2.7.0-python3.10 | DAG scheduling · installs dbt on startup |

Both Airflow services install `dbt-core==1.10.15` and `dbt-snowflake==1.10.4` on container startup via pip. The `dbt_pipeline/` directory is mounted into the container at `/opt/airflow/dbt`.

---

## Snowflake Infrastructure

Run `snowflake_architecture.sql` as `ACCOUNTADMIN` to set up the warehouse, database, role, and schema:

```sql
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE DBT_WAREHOUSE WITH WAREHOUSE_SIZE = 'X-SMALL';
CREATE DATABASE DBT_DATABASE;
CREATE ROLE DBT_ROLE;

GRANT USAGE ON WAREHOUSE DBT_WAREHOUSE TO ROLE DBT_ROLE;
GRANT ROLE DBT_ROLE TO USER YOUR_SNOWFLAKE_USERNAME;  -- replace with your username
GRANT ALL ON DATABASE DBT_DATABASE TO ROLE DBT_ROLE;

USE ROLE DBT_ROLE;
CREATE SCHEMA DBT_DATABASE.DBT_SCHEMA;
```

---

## dbt Project Config

Staging models materialise as **views** (lightweight, always fresh from source).  
Marts and fact models materialise as **tables** (pre-computed, fast to query).  
All models run on `DBT_WAREHOUSE` (X-Small Snowflake warehouse).

```yaml
staging:
  +materialized: view
  +snowflake_warehouse: DBT_WAREHOUSE
marts:
  +materialized: table
  +snowflake_warehouse: DBT_WAREHOUSE
```

---

## Local Setup

1. **Clone the repo**
```bash
git clone https://github.com/Otajon-Yuldashev/dbt-Snowflake-ELT-Pipeline.git
cd dbt-Snowflake-ELT-Pipeline
```

2. **Set up Snowflake** — run `snowflake_architecture.sql` in your Snowflake account. Replace `YOUR_SNOWFLAKE_USERNAME` with your actual username.

3. **Configure dbt profile** — create `~/.dbt/profiles.yml` (or place inside `dbt_pipeline/`):
```yaml
dbt_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: DBT_ROLE
      database: DBT_DATABASE
      warehouse: DBT_WAREHOUSE
      schema: DBT_SCHEMA
      threads: 1
```

4. **Start Docker Compose**
```bash
docker-compose up
```
Airflow UI will be available at `http://localhost:8080` (admin / admin).

5. **Trigger the DAG** — enable and trigger `tpch_data_pipeline` in the Airflow UI.

---

## DAG Screenshot

![Airflow DAG](ELT%20airflow%20orchestration.png)
