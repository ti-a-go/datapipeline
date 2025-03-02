from pyspark.sql.dataframe import DataFrame
import pandas as pd

from spark_session import spark


def preload():
    clients: DataFrame = spark.read.options(header=True).csv("data/source/cliente.csv")

    transactions_dfs = []
    for i in range(1, 4):
        df = pd.read_csv(f"data/source/transacoes_{i}.zip")
        transactions_dfs.append(df)

    transactions_dfs = pd.concat(transactions_dfs)

    transactions: DataFrame = spark.createDataFrame(transactions_dfs)

    transactions_with_missing_clients = transactions.join(
        clients, [transactions.id_cliente == clients.id], "left_anti"
    )

    missing_client_ids = transactions_with_missing_clients.select(
        "id_cliente"
    ).collect()
    missing_client_ids = [row.id_cliente for row in missing_client_ids]

    filtered_transactions = transactions.filter(
        ~transactions.id_cliente.isin(missing_client_ids)
    )
    filtered_transactions.toPandas().to_csv("data/source/transacoes.csv", index=False)


if __name__ == "__main__":
    preload()
