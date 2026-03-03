# ---- Stage 1: Build fat JAR with sbt ----
FROM eclipse-temurin:11-jdk AS builder

# Install sbt
RUN apt-get update && apt-get install -y curl gnupg2 && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" > /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies first (these layers change less often)
COPY build.sbt .
COPY project/ project/
RUN sbt update

# Copy source and build fat JAR
COPY src/ src/
RUN sbt assembly

# ---- Stage 2: Flink runtime ----
FROM flink:1.18.1-scala_2.12-java11

# Copy fat JAR into Flink's usrlib (auto-loaded on job submission)
COPY --from=builder /build/target/scala-2.12/pqg6-flink-processor.jar /opt/flink/usrlib/pqg6-flink-processor.jar

# Add Hadoop filesystem plugin (enables hdfs:// output when HDFS is available)
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# Create local output dir (used when HDFS is not available)
RUN mkdir -p /opt/flink/output

# Environment (overridable via docker-compose)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV FLINK_OUTPUT_PATH=/opt/flink/output
