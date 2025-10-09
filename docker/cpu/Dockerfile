# docker/cpu/Dockerfile
# ---- Stage 1: build isONclust3 (Rust) ----
FROM rust:1-bullseye AS iso3build
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git pkg-config clang binutils && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 https://github.com/aljpetri/isONclust3.git .
# NOTE: the produced binary is named 'isONclust3' (camel-case)
RUN cargo build --release --locked && \
    install -Dm755 target/release/isONclust3 /opt/iso3/bin/isONclust3 && \
    strip /opt/iso3/bin/isONclust3

# ---- Stage 2: IQ-TREE 3 ----
FROM debian:12-slim AS iqtreebuild
ARG IQTREE_REPO=https://github.com/iqtree/iqtree3.git
ARG IQTREE_REF=master
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git build-essential cmake \
    zlib1g-dev libbz2-dev libeigen3-dev libboost-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src/iqtree
# Option A: clone with submodules (recommended)
RUN git clone --branch "${IQTREE_REF}" --recurse-submodules --shallow-submodules --depth 1 "${IQTREE_REPO}" .
# If you prefer to skip optional extras instead, add:
#   -DIQTREE_WITH_CMAPLE=OFF -DIQTREE_WITH_LSD2=OFF
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build -j"$(nproc)" \
 && cmake --install build --prefix /opt/iqtree
 
 
# ---- Stage 3: runtime with micromamba env ----
FROM mambaorg/micromamba:1.5.9

ENV DEBIAN_FRONTEND=noninteractive MAMBA_DOCKERFILE_ACTIVATE=1
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git pigz gzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create smaller conda environment (no minimap2/samtools/racon)
ENV MAMBA_DEFAULT_ENV=nanomb
ENV PATH=/opt/conda/envs/nanomb/bin:/opt/conda/bin:$PATH
SHELL ["/bin/bash", "-lc"]
ARG CHANS="-c conda-forge -c bioconda -c nanoporetech -c hcc -c defaults"

# Core base env
RUN micromamba create -y -n nanomb $CHANS python=3.11 snakemake-minimal=8.11 mamba && micromamba clean -a -y

# Install main tools
RUN micromamba install -y -n nanomb $CHANS \
    vsearch spoa pysam mafft nanoplot "nanoporetech::fastcat=0.24.1" \
    && micromamba clean -a -y

# Copy isONclust3 and IQ-TREE from build stages
COPY --from=iso3build /opt/iso3/bin/isONclust3 /usr/local/bin/isonclust3
COPY --from=iqtreebuild /opt/iqtree/ /usr/local/iqtree/
RUN ln -sf /usr/local/iqtree/bin/iqtree3 /usr/local/bin/iqtree || true

# Final check
CMD bash -lc 'echo "Tools:"; which vsearch spoa mafft iqtree nanoplot python isonclust3; \
  echo "Versions:"; vsearch --version | head -1; spoa --version; mafft --version | head -1; \
  iqtree -h | head -1 || true; nanoplot --version; python --version; isonclust3 --help | head -n 1 || true; bash'