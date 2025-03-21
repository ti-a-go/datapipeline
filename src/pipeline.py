from data_io import load_clients, load_products, load_transactions, save_data
from spark_session import spark
from normalize import normalize_clients_name
from revenue_by_client import get_revenue_by_client
from client_transactions import get_client_transactions_count
from purchased_products_by_client import get_most_purchased_products_by_client
from most_sold_products import get_most_sold_product_in_period
from active_clients import number_of_active_clients_in_past_months


def pipeline():
    clients = load_clients()
    products = load_products()
    transactions = load_transactions()

    normalized_clients = normalize_clients_name(clients)
    save_data(normalized_clients, "clientes")

    revenue_by_client = get_revenue_by_client(transactions, products)
    save_data(revenue_by_client, "receita_cliente")

    clients_transactions = get_client_transactions_count(transactions)
    save_data(clients_transactions, "transacoes_cliente")

    most_purchased_products_by_client = get_most_purchased_products_by_client(
        transactions
    )
    save_data(most_purchased_products_by_client, "produtos_cliente")

    start_date = "2024-01-01"
    end_date = "2024-12-31"
    limit = 5
    most_sold_product = get_most_sold_product_in_period(
        transactions, start_date, end_date, limit
    )
    save_data(most_sold_product, "produtos_vendidos")

    active_clients = number_of_active_clients_in_past_months(transactions)
    active_clients_df = spark.createDataFrame([(active_clients,)], ["total"])
    save_data(active_clients_df, "clientes_ativos")
