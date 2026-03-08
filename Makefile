# ============================================
# PQG6 — Quantum-Resistant 6G Network Security Analytics
# ============================================
# Top-level Makefile for building, running, and managing all modules.
#
# Usage:
#   make build          Build all Docker images
#   make up             Start all services (local)
#   make up-dev         Start dev profile (Kafka + Flink + Dashboard)
#   make up-ml          Start ML profile (Spark + HDFS + Kafka)
#   make down           Stop all services
#   make simulate       Run NS-3 simulation
#   make train-ml       Run Spark ML training
#   make logs           Follow combined logs
#   make ps             Show running services
#   make test           Run health checks
#   make clean          Remove all data volumes
#   make swarm-deploy   Deploy to Docker Swarm
#   make swarm-rm       Remove Swarm stack

.PHONY: build up up-dev up-ml down simulate train-ml logs ps test clean \
        swarm-deploy swarm-rm build-hadoop build-ns3 build-flink build-spark build-dashboard

# ---- Build ----

build: build-hadoop build-ns3 build-flink build-spark build-dashboard
	@echo "All images built successfully."

build-hadoop:
	@echo "Building Hadoop 3.3.6 custom images..."
	docker build -t hadoop-base:3.3.6 -f modules/hdfs-storage/Dockerfile.hadoop-base modules/hdfs-storage/
	docker build -t hadoop-namenode:3.3.6 -f modules/hdfs-storage/Dockerfile.namenode modules/hdfs-storage/
	docker build -t hadoop-datanode:3.3.6 -f modules/hdfs-storage/Dockerfile.datanode modules/hdfs-storage/

build-ns3:
	@echo "Building NS-3 + liboqs simulator..."
	docker build -t pqg6-ns3-simulator:latest modules/ns3-simulator/

build-flink:
	@echo "Building Flink processor..."
	docker build -t pqg6-flink-processor:latest modules/flink-processor/

build-spark:
	@echo "Building Spark analytics..."
	docker build -t pqg6-spark-analytics:latest modules/spark-analytics/

build-dashboard:
	@echo "Building Dashboard..."
	docker build -t pqg6-dashboard:latest modules/dashboard/

# ---- Run (Local) ----

up:
	docker compose --profile full up -d

up-dev:
	docker compose --profile dev up -d

up-ml:
	docker compose --profile ml up -d

down:
	docker compose --profile full down

# ---- Operations ----

simulate:
	docker compose run --rm ns3-simulator

train-ml:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--class TrainModel \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		--driver-memory 1g --executor-memory 1g \
		/opt/spark/pqg6.jar

train-ml-bigdata:
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--class TrainModel \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		--driver-memory 2g --executor-memory 4g \
		--conf spark.sql.shuffle.partitions=200 \
		/opt/spark/pqg6.jar hdfs://namenode:9000/pqg6/training/pq6g-20gb-dataset.csv

upload-dataset:
	@echo "Uploading 29 GB dataset to HDFS (this may take a while)..."
	docker exec namenode hdfs dfs -mkdir -p /pqg6/training
	docker cp data/ns3-output/pq6g-20gb-dataset.csv namenode:/tmp/dataset.csv
	docker exec namenode hdfs dfs -put -f /tmp/dataset.csv /pqg6/training/pq6g-20gb-dataset.csv
	docker exec namenode rm /tmp/dataset.csv
	@echo "Upload complete."

classify:
	docker exec -d spark-master /opt/spark/bin/spark-submit \
		--master local[2] \
		--class ClassifyFlows \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		/opt/spark/pqg6.jar
	@echo "ClassifyFlows running in background."

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	@echo "=== Kafka ===" && \
	docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "OK" || echo "FAIL"
	@echo "=== HDFS ===" && \
	docker compose exec namenode hdfs dfs -ls / > /dev/null 2>&1 && echo "OK" || echo "FAIL"
	@echo "=== Flink ===" && \
	curl -sf http://localhost:8081/overview > /dev/null && echo "OK" || echo "FAIL"
	@echo "=== Dashboard ===" && \
	curl -sf http://localhost:3000 > /dev/null && echo "OK" || echo "FAIL"
	@echo "=== Spark ===" && \
	curl -sf http://localhost:8080 > /dev/null && echo "OK" || echo "FAIL"

clean:
	docker compose down -v
	@echo "All volumes removed."

# ---- Swarm ----

swarm-deploy:
	docker stack deploy -c docker-compose.swarm.yaml pqg6

swarm-rm:
	docker stack rm pqg6

swarm-status:
	@echo "=== Nodes ===" && docker node ls
	@echo "=== Services ===" && docker stack services pqg6
	@echo "=== Tasks ===" && docker stack ps pqg6

