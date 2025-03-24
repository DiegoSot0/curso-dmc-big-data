# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType


# COMMAND ----------

#variables
spark = SparkSession.builder.getOrCreate()

#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_dsm"
path_lakehouse=f'gs://{name_bucket}/produccion/dmc'
path_landing = f"{path_lakehouse}/landing/transacciones/transacciones.data"
path_bronze = f"{path_lakehouse}/bronze/transacciones/"

# COMMAND ----------

print(path_landing)
# gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/transacciones/transacciones.data

# COMMAND ----------


# 6.1 Estructura del dataframe.
schema = StructType([
StructField("ID_PERSONA", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
StructField("MONTO", DoubleType(),True),
StructField("FECHA", StringType(),True)
])

# COMMAND ----------

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(schema).load(path_landing)
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").save(path_bronze)
#gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data 