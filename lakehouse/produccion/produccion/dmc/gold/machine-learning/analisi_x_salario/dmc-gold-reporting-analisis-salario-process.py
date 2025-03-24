# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import *
from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#variables
spark = SparkSession.builder.getOrCreate()

#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_dsm"
path_lakehouse=f'gs://{name_bucket}/produccion/dmc'
#path_persona_landing = f"{path_lakehouse}/landing/persona/persona.data"
path_silver_personas = f"{path_lakehouse}/silver/personas/"
path_silver_empresas = f"{path_lakehouse}/silver/empresas/"
path_gold = f"{path_lakehouse}/gold/machine-learning/analisis_x_salario/"

# COMMAND ----------

df_personas = spark.read.format("delta").load(path_silver_personas)

df_empresas = spark.read.format("delta").load(path_silver_empresas)

# COMMAND ----------

display(df_personas)
display(df_empresas)

# COMMAND ----------

df_personas.createOrReplaceTempView("tb_personas")
df_empresas.createOrReplaceTempView("tb_empresas")

# COMMAND ----------

sql = """ SELECT p.periodo,e.empresa_name as nomnbre_empresa, p.salario, p.edad
            FROM tb_personas p
            inner join tb_empresas e on e.ID = p.ID_EMPRESA and e.periodo = p.periodo
        order by P.ID_EMPRESA"""
df_result_1 = spark.sql(sql)

# COMMAND ----------

display(df_result_1)

# COMMAND ----------

df_result_1.createOrReplaceTempView("tb_analisis_detalle")

# COMMAND ----------

sql_final = """ select periodo
,nomnbre_empresa
,sum(salario) as sum_salario,avg(salario) as avg_salario, avg(edad) as avg_edad from tb_analisis_detalle group by periodo
,nomnbre_empresa
"""

df_result_final = spark.sql(sql_final)

display(df_result_final)

# COMMAND ----------

df_result_final.write.mode("overwrite").format("delta").save(path_gold)

# COMMAND ----------

df = spark.read.format("delta").load(path_gold)
display(df)