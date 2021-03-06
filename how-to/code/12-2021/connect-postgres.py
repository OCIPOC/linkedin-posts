"""
Author: Carlos Barbosa
Linkedin: https://www.linkedin.com/in/carlosbpy/
"""

#======================================================================================================
#                                 HOW TO CONNECT POSTGRES WITH PYSPARK?                               #
#======================================================================================================

#repo maven jars for postgres: https://mvnrepository.com/artifact/org.postgresql/postgresql/42.2.19
#lib: https://pypi.org/project/python-dotenv/

from pyspark.sql import SparkSession
from dotenv      import load_dotenv

load_dotenv()

username   = os.environ.get("POSTGRES_USER")
password   = os.environ.get("POSTGRES_PASSWORD")
host       = os.environ.get("POSTGRES_HOST")
port       = os.environ.get("POSTGRES_PORT")
database   = os.environ.get("POSTGRES_DB")
table      = os.environ.get("POSTGRES_TABLE")
jars_pgsql = os.environ.get("POSTGRES_JARS") #jar path location

spark = (
	SparkSession
    .builder
    .appName("connect-jdbc-pgsql")
    .config("spark.jars", path_jars_pgsql)
    .getOrCreate()
)

device_df = (
  spark.read
    .format("jdbc")
    .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
    .option("dbtable", table)
    .option("user", username)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .load()

)

device_df.show()
device_df.printSchema()

# spark-submit --jars $POSTGRES_JARS connect-pgsql.py