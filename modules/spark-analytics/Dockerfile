# ---- Stage 1: Build fat JAR with sbt ----
FROM eclipse-temurin:11-jdk AS builder

# Install sbt
RUN apt-get update && apt-get install -y curl gnupg2 && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" > /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies
COPY build.sbt .
COPY project/ project/
RUN sbt update

# Build fat JAR
COPY src/ src/
RUN sbt assembly

# ---- Stage 2: Spark submit image ----
FROM eclipse-temurin:11-jre-jammy

# Install Python (for PySpark compatibility) and Hadoop client tools
RUN apt-get update && apt-get install -y python3 curl && \
    rm -rf /var/lib/apt/lists/*

# Copy the fat JAR
COPY --from=builder /build/target/scala-2.12/pqg6-spark-analytics.jar /opt/spark/apps/pqg6-spark-analytics.jar

# Environment
ENV HDFS_NAMENODE=hdfs://namenode:9000
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV SPARK_MASTER=spark://spark-master:7077

# The JAR will be submitted via spark-submit from the Spark cluster
ENTRYPOINT ["echo", "Use spark-submit to run: spark-submit --master spark://spark-master:7077 --class TrainModel /opt/spark/apps/pqg6-spark-analytics.jar"]
