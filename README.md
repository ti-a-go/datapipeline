# Title

Description

# How to run

This project uses a Makefile to define some script responsible to run the docker services.

## Dependencies:

[GNU Make](https://www.gnu.org/software/make/manual/make.html)

[Docker](https://docs.docker.com/)

[Python](https://docs.python.org/3/)


## Available scripts

### Pre-process data to be able to import it into the database.

```sh
make preload
```

This is necessary due to inconsistent data that has transactions without a `client id`.

[This script](./src/preload.py) is responsible to find those transactions and remove them.

### Create the databases and load the initical data into the source database.

This process takes a while as the transactions data is quite large. So make sure in the docker logs that the database finished to copy the data from the `csv` files before to run the next script.

```sh
make db-up
```

### Run the pipeline

This script is responsible to create and run the service responsible for the data pipeline.

```sh
make pipeline
```

# How the pipeline is built

It uses PySpark to parallelize data processing. In this project the parallelization used is the default one, but in a production environment it's possible to manage it to better performance, like the number of partitions used to process the data.

The pipeline is composed of **Spark jobs** that perform one data transformation:
- normalize: concatenate `nome` + `sobrenome` and capitalize it. 
- count the number of `transacoes` for each `cliente`
- calculate the revenue for each `cliente`
- etc

# How to improve the pipeline

Here the pipeline is built sequencially, but it can be executed separately, wich allows us to parallelize the execution of each **spark job**.

It's possible to run them separately in this project by running:

```sh
make spark
```

So it's possible to deploy each **Spark job** independently and parallelize them to improve the performance in a production Spark environment.

## Scale some jobs horizontally

Some jobs can have multiple instances that can run in parallel to increase the performance. It's the case of the **normalization job**. We can divide the data in batches and performa the normalization in parallel.

Others jobs need to be modified so they can be parallelized. It's the case of the job that calculates the revenue by clients. It's currently built to get all the data from the transactions table to make the calculation.

In order to parallelize it, it's necessary firts to create another service to read from the clients table and to publish it in batches. Then, the spark job can read the published batches and get the data from the transactions table only for the clients present in the batch it's reading at the moment. In this case, a index on the `id_cliente` column at the table `transacoes` is a good choice to improve the search performance. This way it's possible to parallelize the spark job that read the published batches.

The same approatch can be used for other spark jobs like the one that calculates the number of transactions by client and the one that finds the most purchased products by client.