from pyspark.sql.dataframe import DataFrame

from load_data import load_clients, load_products, load_transactions
from transform_data import (
    get_revenue_by_client,
    normalize_clients_name,
    get_client_transactions_count,
    get_most_purchased_products_by_client,
    get_most_sold_product_in_period,
    number_of_active_clients_in_past_months,
)


def print_data(title: str, data: DataFrame):
    print("=" * 100)
    print(title)
    print("=" * 100)
    data.printSchema()
    print(data.count())
    print(data.show())


def main():
    clients = load_clients()
    products = load_products()
    transactions = load_transactions()

    normalized_clients = normalize_clients_name(clients)
    print_data("Nomes de clientes normalizados", normalized_clients)

    revenue_by_client = get_revenue_by_client(transactions, products)
    revenue_by_client = revenue_by_client.join(
        normalized_clients, revenue_by_client.id_cliente == normalized_clients.id
    ).drop("id")
    print_data("Receita por cliente", revenue_by_client)

    clients_transactions = get_client_transactions_count(transactions)
    clients_transactions = clients_transactions.join(
        normalized_clients, clients_transactions.id_cliente == normalized_clients.id
    ).drop("id")
    print_data("Número de transações por cliente", clients_transactions)

    most_purchased_products_by_client = get_most_purchased_products_by_client(
        transactions
    )
    print_data("Produtos mais comprados por cliente", most_purchased_products_by_client)

    start_date = "2024-01-01"
    end_date = "2024-12-31"
    limit = 5
    most_sold_product = get_most_sold_product_in_period(
        transactions, start_date, end_date, limit
    )
    most_sold_product = most_sold_product.join(
        products, most_sold_product.id_produto == products.id
    ).drop("id")
    print_data(
        f"{limit} produtos mais vendidos entre {start_date} e {end_date}",
        most_sold_product,
    )

    active_clients = number_of_active_clients_in_past_months(transactions)
    print("=" * 100)
    print(f"Clientes ativos: {active_clients}")
    print("=" * 100)
    # df = concat_clients_name(clients)
    # df = capitalize_clients_name(df)

    # print(df.show())

    # Falta multiplicar o preço do produto pela quantidade
    # df = join_transactions_and_products(load_transactions(), load_products())
    # print(get_revenue_by_client(df).show())

    # df = get_client_transactions_count(load_transactions())
    # print(df.show())

    # df = get_purchased_products_by_client(load_transactions())
    # print(df.show())
    # print(df.filter(df["id_cliente"] == 4619).show())
    # print(df.groupBy("id_cliente").agg(max("produtos_comprados").alias("produto_mais_comprado")).show())

    # print(df.filter(df["nome"] == "Wind").show())

    # df = df.drop("data_transacao")
    # df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()


if __name__ == "__main__":
    main()
