spark:
	docker compose --profile spark up --force-recreate

pipeline:
	docker compose --profile pipeline up --force-recreate

db-up:
	docker compose --profile db up

db-down:
	docker compose --profile db down

db-clear:
	docker compose --profile db down -v

test:
	docker compose --profile test up --force-recreate

preload:
	mkdir -p -m +002 data && \
	wget -P data https://raw.githubusercontent.com/magazord-plataforma/data_engineer_test/refs/heads/master/cliente.csv \
		https://raw.githubusercontent.com/magazord-plataforma/data_engineer_test/refs/heads/master/produtos.csv \
		https://github.com/magazord-plataforma/data_engineer_test/raw/refs/heads/master/transacoes_1.zip \
		https://github.com/magazord-plataforma/data_engineer_test/raw/refs/heads/master/transacoes_2.zip \
		https://github.com/magazord-plataforma/data_engineer_test/raw/refs/heads/master/transacoes_3.zip
	docker compose --profile preload up --force-recreate