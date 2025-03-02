import findspark
from pyspark.sql import SparkSession


findspark.init()


spark = (
    SparkSession.builder.master("local[*]")
    .appName("Data pipeline: Clients Transactions")
    .config("spark.jars", "/usr/postgres-jdbc-driver/postgresql-42.7.4.jar")
    .getOrCreate()
)
