--Estrutura dos dados origem
--MSCK REPAIR TABLE bovespa.dados_origem
select * from dados_origem limit 10

--Estrutura da base especializada 
--MSCK REPAIR TABLE bovespa.spec_bovespa
select * from spec_bovespa limit 10


--Análise 01: quantidade de ativos registrados em 2026
SELECT 
  count(distinct cod_papel) as qtd_papeis
FROM spec_bovespa
WHERE cast(particao as int) between 20260101 and 20260109

--Análise 02: ativo com maior quantidade de negociações em 2026
SELECT 
  cod_papel,
  max(quant_negociacoes) AS quantidade_negociacoes
FROM spec_bovespa
WHERE cast(particao as int) between 20260101 and 20260109
GROUP BY cod_papel
order by quantidade_negociacoes desc 
limit 1


--Análise 03: volume de negociações médio por ativo
SELECT 
  cod_papel,
  AVG(quant_negociacoes) AS volume_medio
FROM spec_bovespa
WHERE cast(particao as int)  between 20260101 and 20260109
GROUP BY cod_papel
order by volume_medio desc

