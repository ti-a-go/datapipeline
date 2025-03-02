spark-build:
	docker compose --profile spark up --build

spark-up:
	docker compose --profile spark up --force-recreate

db-up: preload
	docker compose --profile db up --build -d

db-down:
	docker compose --profile db down

db-clear:
	docker compose --profile db down -v

test-build:
	docker compose --profile test up --build --force-recreate

test:
	docker compose --profile test up --force-recreate

preload:
	docker compose --profile preload up --force-recreate