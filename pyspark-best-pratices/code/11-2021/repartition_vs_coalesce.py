"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#==========================================================================
#                                     BAD                                 #
#==========================================================================

(
  df
  .repartition(3)
  .write
  .mode("overwrite")
  .format("parquet")
  .save("s3a://your-datalake")
)

#==========================================================================
#                                     GOOD                                #
#==========================================================================

(
  df
  .coalesce(3)
  .write
  .mode("overwrite")
  .format("parquet")
  .save("s3a://your-datalake")
)