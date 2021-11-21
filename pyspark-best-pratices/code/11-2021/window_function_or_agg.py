"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/

Post: https://www.linkedin.com/posts/carlosbpy_dataengineering-codequality-activity-6867403529254952960-jy9e
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