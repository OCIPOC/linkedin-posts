"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
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