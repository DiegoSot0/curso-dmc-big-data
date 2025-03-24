# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType


# COMMAND ----------

#variables
spark = SparkSession.builder.getOrCreate()

#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_dsm"
path_lakehouse=f'gs://{name_bucket}/produccion/dmc'
path_persona_landing = f"{path_lakehouse}/landing/persona/persona.data"
path_persona_bronze = f"{path_lakehouse}/bronze/personas/"


# COMMAND ----------

print(path_persona_bronze)
# gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data

# COMMAND ----------


# Todo debe ser string
schema = StructType([
StructField("ID", StringType(),True),
StructField("NOMBRE", StringType(),True),
StructField("TELEFONO", StringType(),True),
StructField("CORREO", StringType(),True),
StructField("FECHA_INGRESO", StringType(),True),
StructField("EDAD", StringType(),True),
StructField("SALARIO", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
])

# COMMAND ----------

df_personas = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(schema).load(path_persona_landing)
display(df_personas)

# COMMAND ----------

df_personas.write.mode("overwrite").format("delta").save(path_persona_bronze)
#gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data 