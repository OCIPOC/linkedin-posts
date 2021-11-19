"""
O motivo no qual é possível realizar encadeamento de expressões no PySpark, 
é porque o mesmo foi desenvolvido a partir do Apache Spark, que vem de linguagens JVM BASED. 
Isso significa que alguns padrões de projetos foram herdados, especificamente a capacidade de encadeamento de expressões.
No entanto, Python não oferece suporte a expressões com várias linhas normalmente e as únicas alternativas são fornecer quebras de linha explícitas, dai você utiliza a famosa \ "contrabarra" ou "barra inversa". 
Mas será que precisa mesmo?

Só tem essa forma de se organizar encadeamento de expressões? 

é uma boa prática fazer isso?
Existe uma forma mais interessante de se organizar e deixar seu código mais fácil de dar manutenção, apenas adicione toda a expressão em um único bloco de parênteses e evite usar \ "contrabarra".
#dataengineering #codequality 
"""


#==============================================
#                       BAD                   #
#==============================================
df = df.filter(F.col('event') == 'executing')\
    .filter(F.col('has_tests') == True)\
    .drop('has_tests')


#==============================================
#                       GOOD                  #
#==============================================
df = (
  df
  .filter(F.col('event') == 'executing')
  .filter(F.col('has_tests') == True)
  .drop('has_tests')
)