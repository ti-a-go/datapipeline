import time, logging

from pyspark.sql.dataframe import DataFrame

from data_io import load_transactions, save_data



logger = logging.getLogger(__name__)

logging.basicConfig(filename='logs/purchased_products_by_client.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)


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

if __name__ == '__main__':
    start_time = time.time()

    transactions = load_transactions()
    most_purchased_products_by_client = get_most_purchased_products_by_client(
        transactions
    )
    save_data(most_purchased_products_by_client, "produtos_cliente")


    execution_time = time.strftime("%M:%S", time.gmtime(time.time() - start_time))
    logger.info(f'Execution time: {execution_time}')
    