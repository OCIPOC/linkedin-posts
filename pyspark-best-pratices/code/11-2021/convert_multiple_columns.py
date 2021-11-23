"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#==========================================================================
#                                     BAD                                 #
#==========================================================================
df = df.withColumn("user_id", df["user_id"].cast("int"))
df = df.withColumn("evaluation_grade", df["evaluation_grade"].cast("int"))
df = df.withColumn("type_user", df["type_user"].cast("int"))
#==========================================================================
#                                  BETTER                                 #
#==========================================================================
(
    df.select(
        F.col("evaluation_grade").cast("int"), 
        F.col("type_user").cast("int"), 
        F.col("user_id").cast("int")
    )
)
#==========================================================================
#                                     GOOD                                #
#==========================================================================
# list of columns that need to be converted from string to integer 
cols_to_convert_to_int = ["user_id", "evaluation_grade", "type_user"]

(
    df.select(
        *(
        F.col(col).cast("int").alias(col)
        for col in cols_to_convert_to_int
        )
    )
)