######################################################
# Importações default
######################################################

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

######################################################
# Importações para o projeto
######################################################

from datetime import date
import pandas as pd
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import date_format
from pyspark.sql import functions as F
from pyspark.sql.window import Window

######################################################
#Configuração das variáveis locais
######################################################

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

hoje = date.today().isoformat()  # Ex: '2025-12-21'

######################################################
# Leitura de dados
######################################################

# Ler dados da tabela do Glue Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="bovespa",
    table_name="dados_origem",
    transformation_ctx="dynamic_frame"
)

#Transforma dynamic frame em data frame para trabalharmos na engenharia de feature
df = dynamic_frame.toDF()

#Filtra apenas a particao correspondente ao dia atual, ou seja, ao ultimo dado inserido 
#Assim, nao havera duplicacao por conta da leitura indeivda dos dados historicos
df_nova_particao = df.filter(F.col("particao") == hoje)

######################################################
# Renomeia colunas
######################################################

#Renomear colunas existentes
df_nova_particao = df.withColumnRenamed("Date", "data_hora") \
       .withColumnRenamed("Ticker", "cod_papel") \
       .withColumnRenamed("Open", "abertura") \
       .withColumnRenamed("High", "valor_max") \
       .withColumnRenamed("Low", "valor_min") \
       .withColumnRenamed("Close", "valor_fechamento") \
       .withColumnRenamed("Volume", "quant_negociacoes")

######################################################
# Cria coluna de data a partir da coluna data_hora
#####################################################

df_nova_particao = df_nova_particao.withColumn(
    "data", 
    date_format("data_hora", "yyyyMMdd")
)

######################################################
# Engenharia de features
######################################################

#----------------------------------------------------
#VARIAVEL 1
#----------------------------------------------------
#Calcula a variacao entre o valor mínimo e máximo do dia 
df_nova_particao = df_nova_particao.withColumn("amplitude", F.col("valor_max") - F.col("valor_min"))

#----------------------------------------------------
#VARIAVEL 2
#----------------------------------------------------
# Criar uma janela para cada ticker, ordenada por data

windowSpec = Window.partitionBy("cod_papel").orderBy("data")

df_nova_particao = df_nova_particao.withColumn("Variacao_1D", (F.col("valor_fechamento") - F.lag("valor_fechamento", 1).over(windowSpec)) / F.lag("valor_fechamento", 1).over(windowSpec) * 100) # Variação em relação ao dia anterior
df_nova_particao = df_nova_particao.withColumn("Variacao_30D", (F.col("valor_fechamento") - F.lag("valor_fechamento", 30).over(windowSpec)) / F.lag("valor_fechamento", 30).over(windowSpec) * 100) # Variação em relação -30d
df_nova_particao = df_nova_particao.withColumn("Variacao_60D", (F.col("valor_fechamento") - F.lag("valor_fechamento", 60).over(windowSpec)) / F.lag("valor_fechamento", 60).over(windowSpec) * 100) # Variação em relação -60D

#----------------------------------------------------
#VARIAVEL 3
#----------------------------------------------------
# Maximo e mínimo nos ultimos 30 dias

window_30d = (
    Window
    .partitionBy("cod_papel")
    .orderBy("data")
    .rowsBetween(-29, 0)
)

df_nova_particao = df_nova_particao.withColumn(
    "max_fechamento_30d",
    F.max("valor_fechamento").over(window_30d)
)

df_nova_particao = df_nova_particao.withColumn(
    "min_fechamento_30d",
    F.min("valor_fechamento").over(window_30d)
)

######################################################
# LOAD
######################################################

# Salva dados no S3 em formato Parquet particionados por dia e cod_papel
dynamic_frame = DynamicFrame.fromDF(df_nova_particao, glueContext, "dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://pipeline-etl-bovespa/refined/",
        "partitionKeys": ["particao","cod_papel"]
    },
    format="parquet"
)

## Finalizar job
job.commit()