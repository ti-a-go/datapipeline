#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    COPY clientes FROM '/usr/data/cliente.csv' CSV HEADER DELIMITER ',';
    COPY produtos FROM '/usr/data/produtos.csv' CSV HEADER DELIMITER ',';
    COPY transacoes_temp FROM PROGRAM 'unzip -p /usr/data/transacoes_1.zip' CSV HEADER DELIMITER ',';
    COPY transacoes_temp FROM PROGRAM 'unzip -p /usr/data/transacoes_2.zip' CSV HEADER DELIMITER ',';
    COPY transacoes_temp FROM PROGRAM 'unzip -p /usr/data/transacoes_3.zip' CSV HEADER DELIMITER ',';
    INSERT INTO transacoes SELECT * FROM transacoes_temp WHERE transacoes_temp.id_cliente in (SELECT id FROM clientes);
EOSQL
