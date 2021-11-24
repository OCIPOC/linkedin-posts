"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#======================================================================================
#                                        BAD                                          #
#======================================================================================

df = (
  spark
  .read
  .format("parquet")
  .load("s3a://your-datalake")
)

(
  df
  .write
  .mode("overwrite")
  .format("delta")
  .save("s3a://your-deltalake")
)

#======================================================================================
#                                        GOOD                                         #
#======================================================================================

from delta.tables import DeltaTable

deltaTable = DeltaTable.convertToDelta(spark, f"parquet.`../your-datalake/`") # location datalake