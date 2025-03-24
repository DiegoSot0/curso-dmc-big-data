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
path_bronze = f"{path_lakehouse}/bronze/empresas/"
path_silver = f"{path_lakehouse}/silver/empresas/"

#captura del periodo de procesamiento
datetime_server = datetime.now()
periodo_actual = datetime_server.strftime("%Y%m")
datetime_previo_1m = datetime_server - relativedelta(months=1)
periodo_previo_1m = datetime_previo_1m.strftime("%Y%m")

# COMMAND ----------

print(path_silver)
# gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data

# COMMAND ----------

df = spark.read.format("delta").option("header","true").load(path_bronze)
display(df)

# COMMAND ----------

df_t = df.withColumn('EMPRESA_NAME',upper(col('EMPRESA_NAME')))\
.withColumn("PERIODO",date_format(add_months(current_date(),-1),"yyyyMM"))

display(df_t)
#gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data 

# COMMAND ----------

#Casteo de datos
df_c = df_t.withColumn("ID",col("ID").cast(IntegerType()))
display(df_c)

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("periodo").format("delta").save(path_silver)