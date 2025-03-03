import os, logging, time
from datetime import date
from dateutil.relativedelta import relativedelta

from pyspark.sql.dataframe import DataFrame

from data_io import load_transactions, save_data
from spark_session import spark

logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="logs/active_clients.log",
    filemode="a",
    format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


def number_of_active_clients_in_past_months(
    transactions: DataFrame, months: int = None
):
    if months is None:
        months = 3
    reference_date = date.today() + relativedelta(months=-months)
    filtered_data = transactions.filter(transactions["data_transacao"] > reference_date)
    return filtered_data.select("id_cliente").distinct().count()


if __name__ == "__main__":
    start_time = time.time()

    months = os.environ.get("ACTIVE_CLIENTS_MONTH", 3)
    try:
        months = int(months)
    except Exception as e:
        logger.error(
            "Failed to load ACTIVE_CLIENTS_MONTH. Make sure this is a valid integer number"
        )
        logger.error(f"Excecption: {str(e)}")
        raise e

    transactions = load_transactions()
    active_clients = number_of_active_clients_in_past_months(transactions, months)
    active_clients_df = spark.createDataFrame([(active_clients,)], ["total"])
    save_data(active_clients_df, "clientes_ativos")

    execution_time = time.strftime("%M:%S", time.gmtime(time.time() - start_time))
    logger.info(f"Execution time: {execution_time}")
