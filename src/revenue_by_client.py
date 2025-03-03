import time, logging

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, sum

from data_io import save_data, load_products, load_transactions


logger = logging.getLogger(__name__)

logging.basicConfig(filename='logs/revenue_by_client.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)


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

if __name__ == '__main__':
    start_time = time.time()


    transactions = load_transactions()
    products = load_products()
    revenue_by_client = get_revenue_by_client(transactions, products)
    save_data(revenue_by_client, "receita_cliente")

    execution_time = time.strftime("%M:%S", time.gmtime(time.time() - start_time))
    logger.info(f'Execution time: {execution_time}')
    