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
	docker compose up pyspark --force-recreate

spark-down:
	docker compose down pyspark

db-up:
	docker compose up database -d

db-down:
	docker compose down database

db-clear:
	docker compose down -v database

test-build:
	docker compose --profile test up --build --force-recreate

test:
	docker compose --profile test up