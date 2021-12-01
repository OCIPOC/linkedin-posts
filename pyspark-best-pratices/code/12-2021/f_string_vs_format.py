"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#====================================================================
#                                 BAD                               #
#====================================================================

zone = "landing-zone"
system = "legacy"
type_system = "bank"
table = "user"

df = (
    spark
    .read
    .format("json")
    .load("{0}/{1}/{2}/{3}".format(zone, system, type_system, table))
)

#====================================================================
#                                  GOOD                             #
#====================================================================

df = (
    spark
    .read
    .format("json")
    .load(f"{zone}/{system}/{type_system}/{table}")
)