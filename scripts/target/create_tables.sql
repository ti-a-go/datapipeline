CREATE TABLE clientes (
    nome VARCHAR(100),
    email VARCHAR(100),
    telefone VARCHAR(15),
    id INT PRIMARY KEY
);

CREATE TABLE produtos (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    descricao VARCHAR(256),
    preco DECIMAL(10, 2),
    ean VARCHAR(13)
);

CREATE TABLE transacoes (
    id INT PRIMARY KEY,
    id_cliente INT,
    id_produto INT,
    quantidade INT,
    data_transacao DATE
);

CREATE TABLE receita_cliente (
    id_cliente INT,
    receita DECIMAL(10, 2)
);

CREATE TABLE transacoes_cliente (
    id_cliente INT,
    transacoes INT
);

CREATE TABLE produtos_cliente (
    id_cliente INT,
    id_produto INT,
    quantidade INT
);

CREATE TABLE produtos_vendidos (
    id_produto INT,
    total INT
);

CREATE TABLE clientes_ativos (
    total INT
);