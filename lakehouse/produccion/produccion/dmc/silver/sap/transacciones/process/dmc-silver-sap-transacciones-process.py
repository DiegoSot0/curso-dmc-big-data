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
path_bronze = f"{path_lakehouse}/bronze/transacciones/"
path_silver = f"{path_lakehouse}/silver/transacciones/"


# COMMAND ----------

print(path_silver)
# gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data

# COMMAND ----------

df = spark.read.format("delta").option("header","true").load(path_bronze)
display(df)

# COMMAND ----------

#Casteo de datos

df_c = df.withColumn("ID_PERSONA",col("ID_PERSONA").cast(IntegerType()))\
          .withColumn("ID_EMPRESA",col("ID_EMPRESA").cast(IntegerType()))\
          .withColumn("MONTO",col("MONTO").cast(DoubleType()))\
          .withColumn("FECHA",to_date(col("FECHA"),"yyyy-MM-dd"))\
          .withColumn("ANIO",year(col("FECHA")))\
          .withColumn("MES",month(col("FECHA")))\
          .withColumn("DIA",dayofmonth(col("FECHA")))
display(df_c)
#gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data 

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("ANIO","MES","DIA").format("delta").save(path_silver)