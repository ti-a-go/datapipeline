from datetime import date
from dateutil.relativedelta import relativedelta

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import initcap, concat_ws, sum, count, max, col


def capitalize_clients_name(df: DataFrame) -> DataFrame:
    return df.withColumn("nome", initcap("nome")).drop("sobrenome")


def concat_clients_name(df: DataFrame) -> DataFrame:
    return df.withColumn("nome", concat_ws(" ", df.nome, df.sobrenome))


def normalize_clients_name(df: DataFrame) -> DataFrame:
    df = concat_clients_name(df)
    return capitalize_clients_name(df)


def get_revenue_by_client(transactions: DataFrame, products: DataFrame) -> DataFrame:
    products = products.withColumnRenamed("id", "product_id")

    transactions_products = transactions.join(
        products,
        (transactions.id_produto == products.product_id)
        & (products.product_id == transactions.id_produto),
    )
    transactions_products = transactions_products.drop("product_id")
    transactions_products = transactions_products.withColumn(
        "total", col("preco") * col("quantidade")
    )

    return transactions_products.groupBy("id_cliente").agg(
        sum("total").alias("receita_total")
    )


def get_client_transactions_count(transactions: DataFrame) -> DataFrame:
    return transactions.groupBy("id_cliente").agg(
        count("id").alias("quantidade_de_transacoes")
    )


def get_most_purchased_products_by_client(transactions: DataFrame) -> DataFrame:
    transactions.createOrReplaceTempView("client_product_quantity")
    return transactions.sparkSession.sql(
        """
        SELECT
            id_cliente,
            id_produto,
            quantidade
        FROM
            (SELECT
                id_cliente,
                id_produto,
                quantidade,
                MAX(quantidade) OVER (PARTITION BY id_cliente ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_quantity
                FROM client_product_quantity
            )
        WHERE max_quantity = quantidade
    """
    )


def get_most_sold_product_in_period(
    transactions: DataFrame, start_date, end_date, limit=None
) -> DataFrame:
    if limit is None:
        limit = 5

    filtered_transactions = transactions.filter(
        col("data_transacao").between(start_date, end_date)
    )
    return (
        filtered_transactions.groupBy("id_produto")
        .agg(sum("quantidade").alias("total_vendido"))
        .sort(col("total_vendido").desc())
        .limit(limit)
    )


def number_of_active_clients_in_past_months(
    transactions: DataFrame, months: int = None
):
    if months is None:
        months = 3
    reference_date = date.today() + relativedelta(months=-months)
    filtered_data = transactions.filter(transactions["data_transacao"] > reference_date)
    return filtered_data.select("id_cliente").distinct().count()
