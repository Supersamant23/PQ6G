# ============================================
# PQG6 NS-3 + liboqs Simulator
# ============================================
# Multi-stage build:
#   Stage 1: Build liboqs from source
#   Stage 2: Build NS-3 + PQG6 simulation module
#   Stage 3: Runtime with Python + Kafka producer
#
# Build time: ~30-45 minutes (C++ compilation)
# Image size: ~1.5 GB

# ---- Stage 1: Build liboqs ----
FROM ubuntu:22.04 AS liboqs-builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential cmake git ninja-build \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Clone and build liboqs (Open Quantum Safe)
RUN git clone --depth 1 --branch 0.10.1 https://github.com/open-quantum-safe/liboqs.git /tmp/liboqs && \
    cd /tmp/liboqs && \
    mkdir build && cd build && \
    cmake -GNinja \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DOQS_BUILD_ONLY_LIB=ON \
    .. && \
    ninja && ninja install && ldconfig

# ---- Stage 2: Build NS-3 + PQG6 simulation ----
FROM ubuntu:22.04 AS ns3-builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential cmake git ninja-build \
    python3 python3-dev python3-pip \
    libssl-dev libsqlite3-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy liboqs from stage 1 (installed to /usr/local)
COPY --from=liboqs-builder /usr/local/lib/ /usr/local/lib/
COPY --from=liboqs-builder /usr/local/include/oqs/ /usr/local/include/oqs/
RUN ldconfig

# Download and extract NS-3.41
RUN wget -q https://www.nsnam.org/releases/ns-allinone-3.41.tar.bz2 -O /tmp/ns3.tar.bz2 && \
    cd /opt && tar xjf /tmp/ns3.tar.bz2 && \
    rm /tmp/ns3.tar.bz2 && \
    mv /opt/ns-allinone-3.41/ns-3.41 /opt/ns3

WORKDIR /opt/ns3

# Copy the PQG6 simulation into NS-3's scratch directory
COPY scratch/ /opt/ns3/scratch/pqg6/

# Configure NS-3
RUN cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DNS3_ENABLED_MODULES="core;network;internet;point-to-point;applications;flow-monitor;traffic-control"

# Build NS-3 (separate step for better error visibility and caching)
RUN cmake --build build -j$(nproc)

# ---- Stage 3: Runtime ----
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy liboqs runtime shared libraries
COPY --from=liboqs-builder /usr/local/lib/liboqs* /usr/local/lib/
RUN ldconfig

# Copy NS-3 build artifacts
COPY --from=ns3-builder /opt/ns3/build /opt/ns3/build
COPY --from=ns3-builder /opt/ns3/scratch /opt/ns3/scratch

# Install Python dependencies for Kafka producer
RUN pip3 install --no-cache-dir \
    confluent-kafka==2.3.0 \
    requests==2.31.0

# Copy the Python export script
COPY scripts/ /opt/pqg6/scripts/

WORKDIR /opt/pqg6

# Default: run simulation then export to Kafka
ENTRYPOINT ["/opt/pqg6/scripts/run-simulation.sh"]
