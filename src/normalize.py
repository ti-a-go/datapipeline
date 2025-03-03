import time, logging

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import initcap, concat_ws

from data_io import load_clients, save_data

logger = logging.getLogger(__name__)

logging.basicConfig(filename='logs/normalize.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)


def capitalize_clients_name(df: DataFrame) -> DataFrame:
    return df.withColumn("nome", initcap("nome")).drop("sobrenome")


def concat_clients_name(df: DataFrame) -> DataFrame:
    return df.withColumn("nome", concat_ws(" ", df.nome, df.sobrenome))


def normalize_clients_name(df: DataFrame) -> DataFrame:
    df = concat_clients_name(df)
    return capitalize_clients_name(df)


if __name__ == '__main__':
    start_time = time.time()

    clients = load_clients()
    normalized_clients = normalize_clients_name(clients)
    save_data(normalized_clients, "clientes")

    execution_time = time.strftime("%M:%S", time.gmtime(time.time() - start_time))
    logger.info(f'Execution time: {execution_time}')