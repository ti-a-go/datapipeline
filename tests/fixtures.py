import pytest
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import findspark

from src.spark_service import spark


findspark.init()

fake = Faker()

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Test Data pipeline: Clients Transactions")
    .getOrCreate()
)


@pytest.fixture
def clients() -> DataFrame:
    data = [
        (
            fake.first_name().upper(),
            fake.last_name().upper(),
            fake.email(),
            fake.phone_number(),
            id,
        )
        for id in range(10)
    ]
    cols = ["nome", "sobrenome", "email", "phone", "id"]

    return spark.createDataFrame(data, cols)


@pytest.fixture
def products() -> DataFrame:
    data = [
        (
            id,
            fake.first_name(),
            fake.paragraph(),
            fake.pyfloat(right_digits=2, min_value=1, max_value=100),
            fake.ean(),
        )
        for id in range(10)
    ]
    cols = ["id", "nome", "descricao", "preco", "ean"]

    return spark.createDataFrame(data, cols)


@pytest.fixture
def revenue_by_client_fixture(clients, products) -> tuple[DataFrame]:
    client_ids = [client.id for client in clients.select("id").collect()]
    product_ids = [product.id for product in products.select("id").collect()]
    data = []
    cols = ["id", "id_produto", "id_cliente", "quantidade"]

    for client_id, product_id, id in zip(
        client_ids, product_ids, range(len(client_ids))
    ):
        data.append(
            (
                id,
                product_id,
                client_id,
                fake.pyint(min_value=1, max_value=10),
            )
        )

    return (products, spark.createDataFrame(data, cols))


@pytest.fixture
def client_transactions_count_fixture(clients, products):
    client_ids = [client.id for client in clients.select("id").collect()]
    product_ids = [product.id for product in products.select("id").collect()]

    data = []
    cols = ["id", "id_produto", "id_cliente", "quantidade"]

    for client_id, product_id in zip(client_ids, product_ids):
        for _ in range(fake.pyint(min_value=1, max_value=10)):
            data.append(
                (
                    fake.pyint(min_value=1, max_value=10000000),
                    product_id,
                    client_id,
                    fake.pyint(min_value=1, max_value=10),
                )
            )

    return spark.createDataFrame(data, cols)


@pytest.fixture
def most_purchased_product_fixture(clients, products):
    client_ids = [client.id for client in clients.select("id").collect()]
    product_ids = [product.id for product in products.select("id").collect()]

    client = client_ids[0]

    product_id_1 = product_ids[1]
    product_id_2 = product_ids[2]

    data = []
    cols = ["id", "id_produto", "id_cliente", "quantidade"]

    for product in [product_id_1, product_id_2]:
        data.append(
            (
                fake.pyint(min_value=1, max_value=10000000),
                product,
                client,
                10,
            )
        )

    for client_id, product_id in zip(client_ids, product_ids):
        data.append(
            (
                fake.pyint(min_value=1, max_value=10000000),
                product_id,
                client_id,
                fake.pyint(min_value=1, max_value=2),
            )
        )

    return (spark.createDataFrame(data, cols), product_id_1, product_id_2, client)
