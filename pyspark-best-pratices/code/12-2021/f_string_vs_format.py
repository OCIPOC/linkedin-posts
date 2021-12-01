"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#==============================================
#                       BAD                   #
#==============================================

zone = "landing-zone"
table = "user"

df = (
    spark
    .read
    .format("json")
    .load("{0}/{1}".format(zone, table))
)

#==============================================
#                       GOOD                  #
#==============================================

df = (
    spark
    .read
    .format("json")
    .load(f"{zone}/{table}")
)