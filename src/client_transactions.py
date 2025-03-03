import time, logging

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count

from data_io import load_transactions, save_data


logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="logs/client_transactions.log",
    filemode="a",
    format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


def get_client_transactions_count(transactions: DataFrame) -> DataFrame:
    return transactions.groupBy("id_cliente").agg(
        count("id").alias("quantidade_de_transacoes")
    )


if __name__ == "__main__":
    start_time = time.time()

    transactions = load_transactions()
    clients_transactions = get_client_transactions_count(transactions)
    save_data(clients_transactions, "transacoes_cliente")

    execution_time = time.strftime("%M:%S", time.gmtime(time.time() - start_time))
    logger.info(f"Execution time: {execution_time}")
