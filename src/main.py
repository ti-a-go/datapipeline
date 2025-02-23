from spark_service import load_clients, load_products, load_transactions


def main():
    clients = load_clients()
    products = load_products()
    transactions = load_transactions()

    clients.printSchema()
    products.printSchema()
    transactions.printSchema()


if __name__ == '__main__':
    main()
