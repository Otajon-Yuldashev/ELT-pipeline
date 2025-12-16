# ELT-pipeline

REMEMBER TO SETUP THINGS ACCORDING TO YOUR NEEDS!

Tools used: Snowflake, dbt, Airflow, Docker.

Pulled data to marts and monitored tables in Snowflake in staging view.
Used dbt to transform and load data to the fact tables.
Orchestrated with Airflow, configuration with Docker.
Built generic and singular tests for data quality, applied business logic.

dataset used: tpch_sf1

NOTE: Remember to setup snowflake account and use snowflake_architecture.sql to build snowflake data warehouse, put your snowflake username instead of YOUR_SNOWFLAKE_USERNAME while granting a role.
