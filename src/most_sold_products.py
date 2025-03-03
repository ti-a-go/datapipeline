import os, time
import logging
import datetime

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, sum
from dotenv import load_dotenv

from data_io import load_transactions, save_data

logger = logging.getLogger(__name__)


load_dotenv()

logging.basicConfig(filename='logs/most_sold_products.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

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


if __name__ == '__main__':
    start_time = time.time()

    transactions = load_transactions()
    start_date = os.environ.get('MOST_SOLD_PRODUCTS_START_DATE', "2024-01-01")
    end_date = os.environ.get('MOST_SOLD_PRODUCTS_START_DATE', "2024-12-31")

    try:
        start_date = datetime.date.fromisoformat(start_date)
        end_date = datetime.date.fromisoformat(end_date)
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


    limit = os.environ.get('MOST_SOLD_PRODUCTS_LIMIT', 5)
    try:
        limit = int(limit)
    except Exception as e:
        logger.error('Failed to load MOST_SOLD_PRODUCTS_LIMIT. Make sure this is a valid integer number')
        logger.error(f'Excecption: {str(e)}')
        raise e

    most_sold_product = get_most_sold_product_in_period(
        transactions, start_date, end_date, limit
    )
    save_data(most_sold_product, "produtos_vendidos")

    execution_time = time.strftime("%M:%S", time.gmtime(time.time() - start_time))
    logger.info(f'Execution time: {execution_time}')
