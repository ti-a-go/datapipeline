from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from src.normalize import normalize_clients_name
from src.revenue_by_client import get_revenue_by_client
from src.client_transactions import get_client_transactions_count
from src.purchased_products_by_client import get_most_purchased_products_by_client
from tests.fixtures import (
    clients,
    products,
    revenue_by_client_fixture,
    client_transactions_count_fixture,
    most_purchased_product_fixture,
)


class TestDataTransformation:

    def test_should_concatenate_and_capitalize_name_and_surname_columns(
        self, clients: DataFrame
    ):
        # Given
        client_names = [client.nome for client in clients.select("nome").collect()]
        client_surnames = [
            client.sobrenome for client in clients.select("sobrenome").collect()
        ]

        # When
        normalized_clients = normalize_clients_name(clients)

        # Then
        normalized_clients_names = [
            client.nome for client in normalized_clients.select("nome").collect()
        ]

        for client_name, client_surname, normalize_client_name in zip(
            client_names, client_surnames, normalized_clients_names
        ):
            expected_name = client_name.capitalize()
            expected_surname = client_surname.capitalize()
            expected_fullname = f"{expected_name} {expected_surname}"

            assert normalize_client_name == expected_fullname

    def test_should_get_revenue_by_client(
        self, revenue_by_client_fixture: tuple[DataFrame]
    ):
        # Given
        products, transactions = revenue_by_client_fixture

        # When
        revenue_by_client = get_revenue_by_client(transactions, products)

        # Then
        for i in range(10):
            product = products.filter(col("id") == i)
            price = product.collect()[0].preco

            transaction = transactions.filter(col("id_cliente") == i)
            quantity = transaction.collect()[0].quantidade

            client_revenue = revenue_by_client.filter(col("id_cliente") == i)
            total_client_revenue = client_revenue.collect()[0].receita_total

            expected_revenue = price * quantity

            assert total_client_revenue == expected_revenue

    def test_should_get_transactions_count_by_client(
        self, client_transactions_count_fixture: DataFrame
    ):
        # Given
        transactions = client_transactions_count_fixture

        client_id = 1
        expected_client_transactions = transactions.filter(
            col("id_cliente") == client_id
        )

        # When
        transactions_by_client = get_client_transactions_count(transactions)

        # Then
        client_transactions = transactions_by_client.filter(
            col("id_cliente") == client_id
        ).collect()[0]

        assert (
            client_transactions.quantidade_de_transacoes
            == expected_client_transactions.count()
        )

    def test_should_get_most_purchased_product_by_client(
        self, most_purchased_product_fixture
    ):
        # Given
        transactions: DataFrame = most_purchased_product_fixture[0]
        product_id_1: int = most_purchased_product_fixture[1]
        product_id_2: int = most_purchased_product_fixture[2]
        client_id: int = most_purchased_product_fixture[3]

        # When
        most_purchased_product_by_client = get_most_purchased_products_by_client(
            transactions
        )

        # Then
        result = most_purchased_product_by_client.filter(
            col("id_cliente") == client_id
        ).collect()

        for row in result:
            assert row.id_produto in [product_id_1, product_id_2]

        assert len(result) == 2
