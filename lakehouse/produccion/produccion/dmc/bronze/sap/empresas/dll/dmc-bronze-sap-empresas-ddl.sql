-- Databricks notebook source
CREATE EXTERNAL TABLE bronze.empresas(
  ID STRING,
  EMPRESA_NAME STRING
) USING DELTA
LOCATION "gs://dmc_datalake_dde_11_dsm/produccion/dmc/bronze/empresas/"

-- COMMAND ----------

SELECT * FROM BRONZE.empresas