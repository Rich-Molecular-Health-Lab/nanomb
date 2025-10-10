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

# Make shell behavior predictable in RUN steps
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Always know where micromamba stores envs
ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV DEBIAN_FRONTEND=noninteractive

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git pigz gzip ca-certificates curl xz-utils libgomp1 file \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/env

# Create env
RUN micromamba create -y -n nanomb -c conda-forge -c bioconda -c nanoporetech -c hcc -c defaults \
      python=3.11 snakemake-minimal=8.11 vsearch racon spoa pysam nanoporetech::fastcat \
  && micromamba clean -a -y

# Add the missing bits
RUN micromamba install -y -n nanomb -c conda-forge -c bioconda \
      mafft NanoPlot \
  && micromamba clean -a -y

# Make the env active at runtime
ENV MAMBA_DEFAULT_ENV=nanomb
ENV PATH=${MAMBA_ROOT_PREFIX}/envs/nanomb/bin:${MAMBA_ROOT_PREFIX}/bin:${PATH}
# Use the image's activation entrypoint so PATH etc. are correct even when overriding CMD
ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]

# isONclust3
COPY --from=iso3build /opt/iso3/bin/isONclust3 /usr/local/bin/isonclust3

# --- IQ-TREE (link the real binary, not the wrapper) ---
ARG IQTREE_URL
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
  # Prefer an actual ELF binary (avx2 -> sse2 -> any iqtree3*)
  tgt="$(find /opt/iqtree-dist -maxdepth 4 -type f -perm -111 \( -name 'iqtree3_avx2' -o -name 'iqtree3_sse2' -o -name 'iqtree3*' \) \
        -exec sh -c 'file -b \"$1\" | grep -q ELF && echo \"$1\"' _ {} \; | head -n1)"; \
  test -n "$tgt" || { echo "iqtree3 binary not found after extract"; exit 3; }; \
  install -Dm755 "$tgt" /usr/local/bin/iqtree3; \
  ln -sf /usr/local/bin/iqtree3 /usr/local/bin/iqtree; \
  rm -f "/tmp/$fname"

# ---- friendly sanity banner ----
CMD bash -lc '\
  echo "Tools on PATH:"; which vsearch spoa mafft iqtree NanoPlot python isonclust3 || true; \
  echo "Versions:"; \
  vsearch --version | head -1 || true; \
  spoa --version || true; \
  mafft --version | head -1 || true; \
  iqtree -h | head -1 || true; \
  NanoPlot --version || true; \
  python --version || true; \
  isonclust3 --help | head -1 || true; \
  bash'