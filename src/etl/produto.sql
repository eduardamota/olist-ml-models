
WITH tb_join AS (
SELECT DISTINCT
       t2.idVendedor,
       t3.*
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.produto AS t3
ON t2.idProduto = t3.idProduto 

WHERE t1.dtPedido < '{date}'
AND t1.dtPedido >= add_months( '{date}1',-6)
AND t2.idVendedor IS NOT NULL
),

tb_summary as (
SELECT idVendedor,
       avg(COALESCE(nrFotos,0)) AS avgFotos,
       avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS avgVolumeProduto,
       percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS medianaVolumeProduto,
       min(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS minVolumeProduto,
       max(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS maxVolumeProduto, 
 COUNT (DISTINCT CASE WHEN DescCategoria = 'cama_mesa_banho' THEN idProduto end) / COUNT(DISTINCT idProduto) AS  pctCategoriacama_mesa_banho,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'beleza_saude' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriabeleza_saude,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'esporte_lazer' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriaesporte_lazer,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'informatica_acessorios' THEN idProduto end) / COUNT(DISTINCT idProduto) ASpctCategoriainformatica_acessorios,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'moveis_decoracao' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriamoveis_decoracao,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'utilidades_domesticas' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriautilidades_domesticas,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'relogios_presentes' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriarelogios_presentes,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'telefonia' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriatelefonia,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'automotivo' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriaautomotivo,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'brinquedos' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriabrinquedos,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'cool_stuff' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriacool_stuff,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'ferramentas_jardim' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriaferramentas_jardim,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'perfumaria' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriaperfumaria,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'bebes' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriabebes,
 COUNT (DISTINCT CASE WHEN DescCategoria = 'eletronicos' THEN idProduto end) / COUNT(DISTINCT idProduto) AS pctCategoriaeletronicos
FROM tb_join 

GROUP BY idVendedor

)

SELECT '{date}' AS dtReference, 
NOW() AS dtIngestion,
* FROM tb_summary 