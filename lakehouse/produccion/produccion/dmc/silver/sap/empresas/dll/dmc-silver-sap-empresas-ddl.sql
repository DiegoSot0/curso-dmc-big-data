-- Databricks notebook source
CREATE TABLE IF NOT EXISTS silver.empresas1 (
    ID INT,
    NOMBRE STRING,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO DATE,
    EDAD INT,
    SALARIO DOUBLE,
    ID_EMPRESA INT,
    PERIODO STRING,
    SEGMENTO STRING,
    ANIO INT,
    MES INT,
    DIA INT
)
USING DELTA
PARTITIONED BY (ANIO, MES, DIA)
LOCATION 'gs://dmc_datalake_dde_11_dsm/produccion/dmc/silver/empresas1';