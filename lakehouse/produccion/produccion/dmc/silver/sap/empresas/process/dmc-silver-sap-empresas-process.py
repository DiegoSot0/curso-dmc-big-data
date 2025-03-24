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
path_bronze = f"{path_lakehouse}/bronze/personas/"
path_silver = f"{path_lakehouse}/silver/personas/"

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

df_t = df.withColumn("telefono",regexp_replace("telefono",'-',''))
display(df_t)
#gs://dmc_datalake_dde_11_dsm/produccion/dmc/landing/persona/persona.data 

# COMMAND ----------

#La Transformación de datos

df_t = df.withColumn("TELEFONO", regexp_replace('telefono', '-', ''))\
.withColumn("PERIODO",date_format(add_months(current_date(),-1),"yyyyMM"))\
.withColumn("SEGMENTO", when(col("SALARIO")<3500,"Masivo").when((col("SALARIO")>=3500) & (col("SALARIO")<=10000),"Premiun").otherwise("Beyond"))
display(df_t)

# COMMAND ----------

#Casteo de datos
df_c = df_t.withColumn("ID",col("ID").cast(IntegerType()))\
          .withColumn("ID_EMPRESA",col("ID_EMPRESA").cast(IntegerType()))\
          .withColumn("EDAD",col("EDAD").cast(IntegerType()))\
          .withColumn("SALARIO",col("SALARIO").cast(DoubleType()))\
          .withColumn("FECHA_INGRESO",to_date(col("FECHA_INGRESO"),"yyyy-MM-dd"))\
          .withColumn("ANIO",year(col("FECHA_INGRESO")))\
          .withColumn("MES",month(col("FECHA_INGRESO")))\
          .withColumn("DIA",dayofmonth(col("FECHA_INGRESO")))
display(df_c)

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("periodo").format("delta").save(path_silver)