"""
Afinal, do que se trata serialização de objetos?

no nosso dia a dia, armazenamos dados em diversos tipos de estrutura de dados, arrays, tabelas, árvores, classes entre outras. Portanto, quando fazemos isso estamos serializando, isto é, comprimindo os dados em uma série de bytes que contêm apenas informações suficientes para reconstruir os dados em sua forma original. Geralmente serializamos os dados quando:

- Realização da persistência dos dados em um arquivo
- Armazando dados em bancos de dados
- Transferir dados através de uma rede (por exemplo, para outros nós em um cluster)

O Spark adotou como padrão o seu serializador de objetos o "ObjectOutputStream", que geralmente é muito lenta.

No entanto, temos a opção de modificar o serializador padrão em nossas sessões do Spark, e o mais recomendado atualmente é o Kryo Serializer, que é significativamente muito mais rápido que a serialização java "ObjectOutputStream" padrão do Spark, até 10X mais rápido!!!

Aplique em sua SparkSession, sempre que possível.
"""


#===========================================================================
#                                   BAD                                    #
#===========================================================================

from pyspark.sql import SparkSession

spark = (
	SparkSession
    .builder
    .getOrCreate()
)

#===========================================================================
#                                   GOOD                                   #
#===========================================================================

spark = (
	SparkSession
  	.builer
  	.config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  	.getOrCreate()
)