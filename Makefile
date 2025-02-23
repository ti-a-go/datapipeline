docker-up:
	docker compose up

docker-build:
	docker compose up --build

docker-down:
	docker compose down

docker-clear:
	docker compose down -v

spark-build:
	docker compose up pyspark --build

spark-up:
	docker compose up pyspark

db-up:
	docker compose up database

db-down:
	docker compose down database

db-clear:
	docker compose down -v database