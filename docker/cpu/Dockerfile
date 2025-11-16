# ---- Stage 1: build isONclust3 (Rust) ----
FROM rust:1-bullseye AS iso3build
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git pkg-config clang && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 https://github.com/aljpetri/isONclust3.git .
RUN cargo build --release --locked && \
    install -Dm755 target/release/isONclust3 /opt/iso3/bin/isONclust3

# ---- Stage 2: runtime image with micromamba env ----
FROM mambaorg/micromamba:1.5.9

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV MAMBA_ROOT_PREFIX=/opt/conda \
    MAMBA_DEFAULT_ENV=nanomb \
    PATH=/opt/conda/envs/nanomb/bin:/opt/conda/bin:$PATH \
    MPLBACKEND=Agg \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash git curl ca-certificates file gzip pigz xz-utils libgomp1 \
      coreutils findutils gawk sed grep tar procps \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/env

# Create env (include everything you need here to cut solver passes)
RUN micromamba create -y -n nanomb -c conda-forge -c bioconda -c nanoporetech -c defaults \
      python=3.11 snakemake-minimal=8.11 \
      vsearch racon spoa pysam nanoporetech::fastcat \
      mafft nanoplot \
  && micromamba clean -a -y

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]

COPY --from=iso3build /opt/iso3/bin/isONclust3 /usr/local/bin/isONclust3
RUN ln -s /usr/local/bin/isONclust3 /usr/local/bin/isonclust3

# --- IQ-TREE (keep bin folder siblings together) ---
ARG IQTREE_URL=="https://github.com/iqtree/iqtree3/releases/download/v3.0.1/iqtree-3.0.1-Linux.tar.gz"
RUN test -n "$IQTREE_URL" || (echo "ERROR: Set --build-arg IQTREE_URL=<download-url>"; exit 1)
RUN set -euo pipefail; cd /tmp; \
  fname="iqtree.$(basename "$IQTREE_URL")"; \
  curl -fsSL "$IQTREE_URL" -o "$fname"; \
  mkdir -p /opt/iqtree-dist; \
  case "$fname" in \
    *.tar.gz) tar -xzf "$fname" -C /opt/iqtree-dist ;; \
    *.tar.xz) tar -xJf "$fname" -C /opt/iqtree-dist ;; \
    *) echo "Unsupported archive: $fname" >&2; exit 2 ;; \
  esac; \
  bindir="$(find /opt/iqtree-dist -maxdepth 4 -type d -name bin | head -n1)"; \
  test -n "$bindir" || { echo "iqtree bin/ dir not found after extract"; ls -R /opt/iqtree-dist; exit 3; }; \
  cp -a "$bindir/." /usr/local/bin/; \
  chmod +x /usr/local/bin/iqtree3* || true; \
  ln -sf /usr/local/bin/iqtree3 /usr/local/bin/iqtree || true; \
  rm -f "/tmp/$fname"

# ---- friendly sanity banner ----
CMD bash -lc '\
  echo "Tools on PATH:"; which vsearch spoa mafft iqtree NanoPlot python isONclust3 2>/dev/null || true; \
  echo "Versions:"; \
  vsearch --version | head -1 || true; \
  spoa --version || true; \
  mafft --version | head -1 || true; \
  iqtree -h | head -1 || true; \
  NanoPlot --version || true; \
  python --version || true; \
  isONclust3 --help | head -1 || true; \
  bash'