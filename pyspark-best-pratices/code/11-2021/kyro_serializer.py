"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
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