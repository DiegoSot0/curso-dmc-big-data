-- Databricks notebook source
CREATE EXTERNAL TABLE bronze.personas(
  ID STRING,
  NOMBRE STRING,
  TELEFONO STRING,
  CORREO STRING,
  FECHA_INGRESO STRING,
  EDAD STRING,
  SALARIO STRING,
  ID_EMPRESA STRING
)
USING DELTA
LOCATION "gs://dmc_datalake_dde_11_dsm/produccion/dmc/bronze/personas/"