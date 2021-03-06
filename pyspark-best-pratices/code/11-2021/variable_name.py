"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#==========================================================================
#                                     BAD                                 #
#==========================================================================
def get_large_downloads(df):
    return df.where(F.col("size") > 100)

#==========================================================================
#                                     GOOD                                #
#==========================================================================
def get_large_downloads(downloads_zize_greater_than_100: DataFrame) -> DataFrame:
    return downloads_zize_greater_than_100.where(F.col("size") > 100)