#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    COPY clientes FROM '/usr/data/cliente.csv' CSV HEADER DELIMITER ',';
    COPY produtos FROM '/usr/data/produtos.csv' CSV HEADER DELIMITER ',';
    COPY transacoes FROM '/usr/data/transacoes.csv' CSV HEADER DELIMITER ',';
EOSQL
