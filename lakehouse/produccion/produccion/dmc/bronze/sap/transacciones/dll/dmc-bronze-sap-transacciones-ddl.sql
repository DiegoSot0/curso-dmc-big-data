-- Databricks notebook source
CREATE TABLE bronze.transacciones(
  ID_PERSONA STRING,
  ID_EMPRESA STRING,
  MONTO DOUBLE,
  FECHA STRING
)
USING DELTA
LOCATION "gs://dmc_datalake_dde_11_dsm/produccion/dmc/bronze/transacciones/"