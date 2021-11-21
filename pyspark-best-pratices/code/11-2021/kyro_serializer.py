"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/

Post: https://www.linkedin.com/posts/carlosbpy_dataengineering-pyspark-activity-6867789920879095808-LAZA
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