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

FROM base AS release

CMD [ "python3", "src/main.py" ]
