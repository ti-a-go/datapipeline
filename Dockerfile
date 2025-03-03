FROM spark:3.5.4-scala2.12-java17-python3-ubuntu AS base

WORKDIR /usr/app

COPY requirements.txt .

USER root

RUN pip install --no-cache-dir -r requirements.txt && \
    wget -P /usr/postgres-jdbc-driver https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

FROM base AS preload

CMD [ "python3", "src/preload.py" ]

FROM base AS test

CMD [ "pytest" ]

FROM base AS pipeline

CMD [ "python3", "src/main.py" ]

FROM base AS normalize

CMD [ "python3", "src/normalize.py" ]

FROM base AS revenue_by_client

CMD [ "python3", "src/revenue_by_client.py" ]

FROM base AS client_transactions

CMD [ "python3", "src/client_transactions.py" ]

FROM base AS purchased_products_by_client

CMD [ "python3", "src/purchased_products_by_client.py" ]

FROM base AS most_sold_products

CMD [ "python3", "src/most_sold_products.py" ]

FROM base AS active_clients

CMD [ "python3", "src/active_clients.py" ]
