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

# ---- Stage 2: build IQ-TREE (CMake) ----
# Choose repo/ref at build time:
#   --build-arg IQTREE_REPO=https://github.com/iqtree/iqtree3.git --build-arg IQTREE_REF=master   # IQ-TREE 3
#   --build-arg IQTREE_REPO=https://github.com/iqtree/iqtree2.git --build-arg IQTREE_REF=v2.3.5   # IQ-TREE 2 (example)
 FROM debian:12-slim AS iqtreebuild
 ARG IQTREE_REPO=https://github.com/iqtree/iqtree3.git
 ARG IQTREE_REF=master
 RUN apt-get update && apt-get install -y --no-install-recommends \
     ca-certificates git build-essential cmake \
     zlib1g-dev libbz2-dev libeigen3-dev libboost-dev \
  && rm -rf /var/lib/apt/lists/*
 WORKDIR /src/iqtree
 # pull submodules too (shallow)
 RUN git clone --branch "${IQTREE_REF}" --recurse-submodules --shallow-submodules --depth 1 "${IQTREE_REPO}" .
 RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  && cmake --build build -j"$(nproc)" \
  && cmake --install build --prefix /opt/iqtree

# ---- Stage 3: runtime image with micromamba env ----
FROM mambaorg/micromamba:1.5.9
ENV DEBIAN_FRONTEND=noninteractive MAMBA_DOCKERFILE_ACTIVATE=1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git pigz gzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/env
COPY docker/cpu/environment.cpu.yml .
RUN micromamba create -y -n nanomb -f environment.cpu.yml && micromamba clean -a -y

ENV MAMBA_DEFAULT_ENV=nanomb
ENV PATH=/opt/conda/envs/nanomb/bin:/opt/conda/bin:$PATH

# isONclust3 binary
COPY --from=iso3build /opt/iso3/bin/isONclust3 /usr/local/bin/isONclust3
RUN ln -sf /usr/local/bin/isONclust3 /usr/local/bin/isonclust3


# IQ-TREE binaries (could be iqtree2 or iqtree3; expose a stable "iqtree" on PATH)
COPY --from=iqtreebuild /opt/iqtree/ /usr/local/iqtree/
RUN ln -sf /usr/local/iqtree/bin/iqtree3 /usr/local/bin/iqtree || \
    ln -sf /usr/local/iqtree/bin/iqtree2 /usr/local/bin/iqtree

# quick sanity when entering the container
CMD ["bash", "-lc", "echo 'Tools:'; \
  which minimap2 samtools vsearch racon spoa mafft iqtree nanoplot python isonclust3; \
  echo 'Versions:'; \
  minimap2 --version; samtools --version | head -1; vsearch --version | head -1; \
  racon --version; mafft --version | head -1; \
  iqtree -h | head -1 || true; \
  nanoplot --version; python --version; \
  isonclust3 --help | head -n 1; echo; bash"]