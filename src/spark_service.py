import os

import findspark
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

findspark.init()

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Data pipeline: Clients Transactions") \
    .config("spark.jars", "/usr/app/postgresql-42.7.4.jar") \
    .getOrCreate()


def load_table(table_name: str):
    db_name = os.getenv("DATABASE_NAME")
    db_user = os.getenv("DATABASE_USER")
    db_port = os.getenv("DATABASE_PORT")
    db_password = os.getenv("DATABASE_PASSWORD")
    db_host = os.getenv("DATABASE_HOST")
    return spark.read \
            .format('jdbc') \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
            .option("dbtable", table_name) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

def load_clients():
    return load_table('clientes')


def load_products():
    return load_table('produtos')


def load_transactions():
    return load_table('transacoes')