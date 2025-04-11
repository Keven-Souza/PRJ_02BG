6. Desafio Final
Objetivo: Combinar tudo o que foi aprendido. Dado um conjunto de dados fictício
sobre vendas (por exemplo, id_cliente, valor_compra, data_compra):
• Identifique os clientes com maior valor de compra.
• Agrupe as compras por ano e calcule o total de vendas anuais.
• Salve os resultados em um formato de sua escolha (CSV, JSON, etc.).

Resposta:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum as _sum

spark = SparkSession.builder.appName("DesafioFinalVendas").getOrCreate()

df_vendas = spark.read.csv("vendas.csv", header=True, inferSchema=True)

clientes_top = df_vendas.orderBy(col("valor_compra").desc())
clientes_top.show()

df_vendas_com_ano = df_vendas.withColumn("ano", year(col("data_compra")))

vendas_por_ano = df_vendas_com_ano.groupBy("ano").agg(
    _sum("valor_compra").alias("total_vendas")
)
vendas_por_ano.show()

vendas_por_ano.write.mode("overwrite").csv("saida/vendas_por_ano.csv", header=True)
clientes_top.write.mode("overwrite").csv("saida/clientes_top.csv", header=True)


