import os

from dotenv import load_dotenv
from pyspark.sql.dataframe import DataFrame

from spark_service import spark


load_dotenv()


def load_table(table_name: str) -> DataFrame:
    db_name = os.getenv("DATABASE_NAME")
    db_user = os.getenv("DATABASE_USER")
    db_port = os.getenv("DATABASE_PORT")
    db_password = os.getenv("DATABASE_PASSWORD")
    db_host = os.getenv("DATABASE_HOST")
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}")
        .option("dbtable", table_name)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def load_clients() -> DataFrame:
    return load_table("clientes")


def load_products() -> DataFrame:
    return load_table("produtos")


def load_transactions() -> DataFrame:
    return load_table("transacoes")
