-- Databricks notebook source
show catalogs;

-- COMMAND ----------

CREATE  SCHEMA IF NOT EXISTS bronze

LOCATION 'gs://dmc_datalake_dde_11_dsm/produccion/dmc/bronze';

-- COMMAND ----------

CREATE  SCHEMA IF NOT EXISTS silver

LOCATION 'gs://dmc_datalake_dde_11_dsm/produccion/dmc/silver';

-- COMMAND ----------

CREATE  SCHEMA IF NOT EXISTS gold

LOCATION 'gs://dmc_datalake_dde_11_dsm/produccion/dmc/gold';