"""
As famosas funções da janela no Spark "Spark window functions", podem ser aplicadas em todas as linhas do seu DataFrame, utilizando um quadro global. 
Isso é feito especificando zero colunas na partição por expressão, ou seja: W.partitionBy()
No entanto, código como esse deve ser evitado, pois força o Spark a combinar todos os dados em uma única partição e você receberá o WARN: "No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation".
O que pode ser extremamente prejudicial para o desempenho, pois o mesmo não estara distribuindo com eficiência os dados para os nodes do cluster.
Prefira usar agregações sempre que possível e nunca mais veja o WARN acima.
"""

from pyspark.sql import (
    functions as F, 
    Window as W
)

# bad
w = W.partitionBy()
df = (
    df
    .select(
        F.avg('revenue').over(w).alias('revenue'))
)
df.show()