"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#======================================================================================
#                                        BAD                                          #
#======================================================================================

df.toPandas()

#======================================================================================
#                                        BETTER                                       #
#======================================================================================

df.show(vertical=True)

#======================================================================================
#                                        GOOD                                         #
#======================================================================================

spark = (
    SparkSession
    .builder
    .config("spark.sql.repl.eagerEval.enabled", True)
    .getOrCreate()

)

df