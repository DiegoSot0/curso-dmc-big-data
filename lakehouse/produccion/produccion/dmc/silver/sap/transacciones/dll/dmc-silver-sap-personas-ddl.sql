-- Databricks notebook source
CREATE TABLE IF NOT EXISTS silver.transacciones (
    ID_PERSONA INT,
    ID_EMPRESA INT,
    MONTO DOUBLE,
    FECHA DATE,
    ANIO INT,
    MES INT,
    DIA INT
)
USING DELTA
PARTITIONED BY (ANIO, MES, DIA)
LOCATION 'gs://dmc_datalake_dde_11_dsm/produccion/dmc/silver/transacciones';