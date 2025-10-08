# docker/nanoalign/Dockerfile
FROM debian:12-slim

ARG MINIMAP2_VER=2.30
ARG SAMTOOLS_VER=1.22.1
ARG RACON_VER=1.5.0

ENV DEBIAN_FRONTEND=noninteractive

# Build & runtime deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates curl \
    build-essential pkg-config autoconf automake cmake git \
    zlib1g-dev libbz2-dev liblzma-dev libcurl4-openssl-dev libdeflate-dev \
    libncursesw5-dev \
 && rm -rf /var/lib/apt/lists/*

# --- minimap2 (binary) ---
RUN curl -L -o /tmp/minimap2.tar.bz2 \
      https://github.com/lh3/minimap2/releases/download/v${MINIMAP2_VER}/minimap2-${MINIMAP2_VER}_x64-linux.tar.bz2 \
 && mkdir -p /opt/minimap2 \
 && tar -xjf /tmp/minimap2.tar.bz2 -C /opt/minimap2 --strip-components=1 \
 && ln -s /opt/minimap2/minimap2 /usr/local/bin/minimap2 \
 && rm -f /tmp/minimap2.tar.bz2

# --- samtools (source) ---
RUN curl -L -o /tmp/samtools.tar.bz2 \
      https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VER}/samtools-${SAMTOOLS_VER}.tar.bz2 \
 && mkdir -p /tmp/samtools \
 && tar -xjf /tmp/samtools.tar.bz2 -C /tmp/samtools --strip-components=1 \
 && cd /tmp/samtools \
 && ./configure --prefix=/opt/samtools \
 && make -j"$(nproc)" && make install \
 && ln -s /opt/samtools/bin/samtools /usr/local/bin/samtools \
 && rm -rf /tmp/samtools /tmp/samtools.tar.bz2

# --- racon (source) ---
RUN git clone --recursive --branch ${RACON_VER} https://github.com/lbcb-sci/racon.git /tmp/racon \
 && cmake -S /tmp/racon -B /tmp/racon/build -DCMAKE_BUILD_TYPE=Release -Dracon_build_tests=OFF \
 && cmake --build /tmp/racon/build -j"$(nproc)" \
 && cmake --install /tmp/racon/build \
 && rm -rf /tmp/racon

# sanity check on start
CMD bash -lc 'echo "Tools:"; minimap2 --version; samtools --version | head -1; racon --version'
