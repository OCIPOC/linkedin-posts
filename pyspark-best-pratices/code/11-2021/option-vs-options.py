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
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load("s3a://your-datalake")
)

#======================================================================================
#                                        GOOD                                         #
#======================================================================================

config_file = {
    "header": True,
    "inferSchema": True,
    "delimiter": ";"
}

df = (
    spark
    .read
    .format("csv")
    .options(**config_file)
    .load("s3a://your-datalake")
)