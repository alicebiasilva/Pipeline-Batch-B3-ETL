CREATE EXTERNAL TABLE IF NOT EXISTS dados_origem ( 
    date timestamp, 
    open double, 
    high double, 
    low double, 
    close double, 
    volume double 
) 
PARTITIONED BY ( particao string ) 
STORED AS PARQUET 
LOCATION 's3://pipeline-etl-bovespa/raw/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

---------------------------------------------------------

CREATE EXTERNAL TABLE spec_bovespa( 
    data_hora timestamp, 
    valor_fechamento double, 
    abertura double, 
    valor_max double, 
    valor_min double, 
    quant_negociacoes double, 
    amplitude double, 
    variacao_1d double, 
    variacao_30d double, 
    variacao_60d double, 
    max_fechamento_30d double, 
    min_fechamento_30d double 
) 
PARTITIONED BY (particao string, cod_papel string ) 
STORED AS PARQUET 
LOCATION 's3://pipeline-etl-bovespa/refined/' 
TBLPROPERTIES ('parquet.compression'='SNAPPY')