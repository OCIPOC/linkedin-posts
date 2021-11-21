"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/

Post: https://www.linkedin.com/posts/carlosbpy_dataengineering-codequality-activity-6867076679475486720-MRlM
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