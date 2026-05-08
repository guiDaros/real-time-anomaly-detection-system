# =========================================
# VARIABLES
# =========================================

COMPOSE=docker compose

PYTHON=poetry run python

# =========================================
# INFRA
# =========================================

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down
	$(COMPOSE) up -d

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

# =========================================
# KAFKA
# =========================================

create-topics:
	docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
	--create \
	--if-not-exists \
	--topic transactions \
	--bootstrap-server localhost:9092 \
	--partitions 1 \
	--replication-factor 1

list-topics:
	docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
	--list \
	--bootstrap-server localhost:9092

# =========================================
# STREAMING
# =========================================

producer:
	$(PYTHON) streaming/producer.py

stream:
	$(PYTHON) streaming/spark_stream.py

# =========================================
# DEVELOPMENT
# =========================================

format:
	poetry run black .

lint:
	poetry run ruff check .

test:
	poetry run pytest

# =========================================
# CLEANUP
# =========================================

clean:
	rm -rf checkpoints/*
	rm -rf spark-warehouse/*
	rm -rf .pytest_cache/*
	find . -type d -name "__pycache__" -exec rm -rf {} +

# =========================================
# FULL LOCAL START
# =========================================

run:
	make up
	sleep 10
	make create-topics