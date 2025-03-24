-- Databricks notebook source
CREATE TABLE IF NOT EXISTS silver.personas1 (
    ID INT NOT NULL,
    EMPRESA_NAME STRING,
    PERIODO STRING
)
USING DELTA
PARTITIONED BY (PERIODO)
LOCATION 'gs://dmc_datalake_dde_11_dsm/produccion/dmc/silver/personas1'
