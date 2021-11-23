"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

from pyspark.sql import (
    functions as F, 
    Window as W
)

# bad
w = W.partitionBy()
df = (
    df
    .select(
        F.avg('revenue').over(w).alias('revenue'))
)
df.show()