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
# Importações específicas para o projeto
######################################################

import yfinance as yf
import pandas as pd
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, current_date, date_format
from pyspark.sql.types import DecimalType

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

######################################################
#Extração das informações de interesse (yfinancial) e inserção do resultado no bucket s3
######################################################

# Lista ações de interesse
lista_de_tickers_b3 = ['ITSA4.SA','PETR4.SA','RAIZ4.SA','BBAS3.SA','B3SA3.SA','CSAN3.SA','VALE3.SA','ITUB4.SA','BBDC4.SA','BRAV3.SA','ABEV3.SA','CPLE5.SA','COGN3.SA','CPLE3.SA','PETR3.SA','BEEF3.SA','ASAI3.SA','VBBR3.SA','GMAT3.SA','VAMO3.SA','CVCB3.SA','CMIG4.SA','RADL3.SA','LREN3.SA','MGLU3.SA','SUZB3.SA','GGBR4.SA','WEGE3.SA','POMO4.SA','AZUL4.SA','EQTL3.SA','CSNA3.SA','NATU3.SA','RDOR3.SA','MOTV3.SA','KLBN11.SA','ENEV3.SA','USIM5.SA','DIRR3.SA','MBRF3.SA','SBSP3.SA','PRIO3.SA','RAIL3.SA','CEAB3.SA','AXIA3.SA','CMIN3.SA','SIMH3.SA','RENT3.SA','PETZ3.SA','BPAC11.SA','MOVI3.SA','GOAU4.SA','PCAR3.SA','SMFT3.SA','HAPV3.SA','BBSE3.SA','TIMS3.SA','BBDC3.SA','RCSL4.SA','RECV3.SA','GUAR3.SA','BRAP4.SA','DXCO3.SA','CYRE3.SA','UGPA3.SA','MRVE3.SA','BRKM5.SA','ONCO3.SA','ALOS3.SA','EMBJ3.SA','CBAV3.SA','ANIM3.SA','TOTS3.SA','KLBN4.SA','AMBP3.SA','AZEV4.SA','SANB11.SA','VIVT3.SA','JHSF3.SA','KEPL3.SA','RAPT4.SA','ALPA4.SA','MULT3.SA','AZZA3.SA','CXSE3.SA','AURE3.SA','CPFE3.SA','AXIA6.SA','PSSA3.SA','GRND3.SA','DASA3.SA','SAPR11.SA','HYPE3.SA','LWSA3.SA','BHIA3.SA','SMTO3.SA','ECOR3.SA','ODPV3.SA','VIVA3.SA','ISAE4.SA','GGPS3.SA','HBSA3.SA','LJQQ3.SA','OIBR3.SA','EGIE3.SA','CURY3.SA','PGMN3.SA','AZTE3.SA','FLRY3.SA','BPAN4.SA','ENGI11.SA','TEND3.SA','CSMG3.SA','EZTC3.SA','SAPR4.SA','SLCE3.SA','VVEO3.SA','IFCM3.SA','IGTI11.SA','IGTI11.SA','ITUB3.SA','TAEE11.SA','INTB3.SA','QUAL3.SA','NEOE3.SA','SBFG3.SA','HBOR3.SA','SOJA3.SA','YDUQ3.SA','MLAS3.SA','CASH3.SA','ORVR3.SA','AMER3.SA','PMAM3.SA','BRSR6.SA','MILS3.SA','VULC3.SA','PLPL3.SA','ALUP11.SA','LOGG3.SA','ITSA3.SA','MYPK3.SA','BMGB4.SA','FRAS3.SA','PRNR3.SA','TUPY3.SA','IRBR3.SA','EVEN3.SA','MDIA3.SA','KLBN3.SA','TTEN3.SA','PNVL3.SA','LIGT3.SA','SEER3.SA','MDNE3.SA','ARML3.SA','BLAU3.SA','DESK3.SA','WIZC3.SA','FESA4.SA','JALL3.SA','CAML3.SA','LAVV3.SA','RANI3.SA','ESPA3.SA','JSLG3.SA','HBRE3.SA','POMO3.SA','RCSL3.SA','MTRE3.SA','USIM3.SA','TRIS3.SA','BIOM3.SA','TFCO4.SA','CSED3.SA','ABCB4.SA','POSI3.SA','ALLD3.SA','MELK3.SA','FIQE3.SA','UNIP6.SA','OPCT3.SA','SAPR3.SA','LEVE3.SA','SYNE3.SA','SEQL3.SA','MATD3.SA','BMOB3.SA','PINE4.SA','VLID3.SA','MEAL3.SA','CSUD3.SA','VTRU3.SA','GFSA3.SA','AGRO3.SA','SHUL4.SA','DEXP3.SA','AZEV3.SA','BRBI11.SA','ROMI3.SA','TAEE4.SA','TASA4.SA','ENJU3.SA','BRST3.SA','CMIG3.SA','VITT3.SA','TGMA3.SA','PFRM3.SA','LPSB3.SA','VIVR3.SA','ETER3.SA','TOKY3.SA','ALPK3.SA','PDTC3.SA','PTBL3.SA','BMEB4.SA','BRAP3.SA','DMVF3.SA','TAEE3.SA','PDGR3.SA','RNEW4.SA','GOAU3.SA','SCAR3.SA','TPIS3.SA','AMAR3.SA','ENGI3.SA','SANB4.SA','NGRD3.SA','AERI3.SA','LAND3.SA','CGRA4.SA','TRAD3.SA','INEP3.SA','BOBR4.SA','TCSA3.SA','ATED3.SA','RAPT3.SA','SHOW3.SA','AVLL3.SA','IGTI3.SA','IGTI3.SA','LOGN3.SA','SANB3.SA','LUPA3.SA','EUCA4.SA','CEBR6.SA','BRKM3.SA','BEES3.SA','INEP4.SA','GGBR3.SA','AMOB3.SA','OFSA3.SA','VSTE3.SA','UNIP3.SA','TASA3.SA','CRPG5.SA','MNPR3.SA','DOTZ3.SA','FICT3.SA','ALUP4.SA','ALPA3.SA','CAMB3.SA','TECN3.SA','WEST3.SA','AGXY3.SA','RNEW3.SA','BPAC5.SA','SNSY5.SA','JFEN3.SA','EPAR3.SA','BSLI3.SA','BRSR3.SA','UCAS3.SA','EQPA3.SA','ENGI4.SA','BEES4.SA','BAZA3.SA','ALUP3.SA','WHRL4.SA','MOAR3.SA','TELB4.SA','CGRA3.SA','MGEL4.SA','HOOT4.SA','EALT4.SA','BALM4.SA','COCE5.SA','PTNT4.SA','ENMT3.SA','CEBR5.SA','WHRL3.SA','CLSC4.SA','HAGA3.SA','MTSA4.SA','REDE3.SA','OIBR4.SA','CTKA4.SA','HAGA4.SA','TELB3.SA','BPAC3.SA','WLMM4.SA','PINE3.SA','RSID3.SA','PPLA11.SA','EMAE4.SA','DEXP4.SA','AALR3.SA','NUTR3.SA','BMEB3.SA','SOND5.SA','EALT3.SA','BSLI4.SA','ISAE3.SA','CEBR3.SA','FHER3.SA','OSXB3.SA','NEXP3.SA','AZEV11.SA','BGIP4.SA','RSUL4.SA','BIED3.SA','CTSA4.SA','RVEE3.SA','RVEE3.SA','PEAB3.SA','PEAB4.SA','GEPA3.SA','SOND6.SA','MRSA6B.SA','EUCA3.SA','GEPA4.SA','CTAX3.SA','CGAS5.SA','MRSA5B.SA','MRSA3B.SA','CEEB3.SA','EQMA3B.SA','BMIN4.SA','UNIP5.SA','CRPG3.SA','BRKM6.SA','MAPT4.SA','BMKS3.SA','FESA3.SA','DOHL4.SA','FIEI3.SA','MNDL3.SA','LUXM4.SA','AFLT3.SA','BIOM11.SA','RPMG3.SA','IGTI4.SA','IGTI4.SA','BAUH4.SA','EKTR4.SA','PATI3.SA','BALM3.SA','NORD3.SA','TKNO4.SA','CEEB5.SA','PATI4.SA','CBEE3.SA','PTNT3.SA','EQPA5.SA','PINE11.SA','AHEB3.SA','ESTR4.SA','CLSC3.SA','HBTS5.SA','F4RIO3.SA','JOPA3.SA','PLAS3.SA','CEDO4.SA','RDNI3.SA','BNBR3.SA','CRPG6.SA','CGAS3.SA','WLMM3.SA','MERC4.SA','MWET4.SA','PSVM11.SA','RPAD5.SA','MSPA4.SA','CTSA3.SA','BGIP3.SA','BRSR5.SA','GSHP3.SA','TKNO3.SA','ADMF3.SA','HETA4.SA','SNSY3.SA','CEDO3.SA','CEED3.SA']

# Download dos dados do yfinancial (retorna um Pandas DataFrame)
df_pandas = yf.download(lista_de_tickers_b3, period="1d") 
df_pandas = df_pandas.stack(level=1, future_stack=True).reset_index()

######################################################
#Tratamento de dados para salvá-los corretamente no s3
######################################################

# Remove linhas totalmente vazias de preços
df_pandas = df_pandas.dropna(
    subset=["Open", "High", "Low", "Close", "Adj Close", "Volume"],
    how="all"
)

#Ajusta tipagem das colunas
df_pandas['Open'] = df_pandas['Open'].astype(float)
df_pandas['High'] = df_pandas['High'].astype(float)
df_pandas['Low'] = df_pandas['Low'].astype(float)
df_pandas['Close'] = df_pandas['Close'].astype(float)
df_pandas['Volume'] = df_pandas['Volume'].astype(float)

#Transforma dataframe pandas em spark
df_spark = spark.createDataFrame(df_pandas)
df_spark = df_spark.drop("adj_close")

# Cria coluna de partição com base na coluna "date"
df_spark = df_spark.withColumn("particao", date_format(col("date"), "yyyyMMdd"))

######################################################
# Salva dados no S3 em formato Parquet
###################################################### 

dynamic_frame = DynamicFrame.fromDF(df_spark, glueContext, "dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://pipeline-etl-bovespa/raw/",
        "partitionKeys": ["particao"]
    },
    format="parquet"
)

# Finalizar job
job.commit()