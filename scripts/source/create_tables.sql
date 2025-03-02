-- Criação da tabela de clientes
CREATE TABLE clientes (
    nome VARCHAR(100),
    sobrenome VARCHAR(100),
    email VARCHAR(100),
    telefone VARCHAR(15),
    id INT PRIMARY KEY
);

-- Criação da tabela de produtos
CREATE TABLE produtos (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    descricao VARCHAR(256),
    preco DECIMAL(10, 2),
    ean VARCHAR(13)
);

-- Criação da tabela de transações
CREATE TABLE transacoes (
    id INT PRIMARY KEY,
    id_cliente INT REFERENCES clientes(id),
    id_produto INT REFERENCES produtos(id),
    quantidade INT,
    data_transacao DATE
);

-- Criação da tabela de transações_temp
-- CREATE TABLE transacoes_temp (
--     id INT PRIMARY KEY,
--     id_cliente INT,
--     id_produto INT,
--     quantidade INT,
--     data_transacao DATE
-- );