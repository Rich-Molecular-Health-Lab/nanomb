# docker/nanotree/Dockerfile
FROM mambaorg/micromamba:1.5.9

# --- System setup ------------------------------------------------------------
USER root

# Base OS dependencies
SHELL ["/bin/sh", "-c"]
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      bash git curl ca-certificates file gzip pigz xz-utils libgomp1 \
      coreutils findutils gawk sed grep tar procps \
    && rm -rf /var/lib/apt/lists/*

# --- Micromamba environment --------------------------------------------------
SHELL ["/usr/local/bin/_dockerfile_shell.sh"]
ENV MAMBA_ROOT_PREFIX=/opt/conda \
    MPLBACKEND=Agg \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive

# Create nanotree env with core tools (OpenMP-only epa-ng from conda)
RUN micromamba create -y -n nanotree -c conda-forge -c bioconda \
      python=3.11 snakemake-minimal=8.11 mafft epa-ng gappa \
  && micromamba clean -a -y

# --- IQ-TREE -----------------------------------------------------------------
ARG IQTREE_URL="https://github.com/iqtree/iqtree3/releases/download/v3.0.1/iqtree-3.0.1-Linux.tar.gz"
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

# --- Build MPI-enabled EPA-ng -----------------------------------------------
USER root
SHELL ["/bin/sh", "-c"]
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      build-essential cmake git \
      openmpi-bin libopenmpi-dev \
      bison flex unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/src
RUN set -eux; \
    git clone --depth 1 https://github.com/pierrebarbera/epa-ng.git; \
    cd epa-ng; \
    make clean; \
    make -j"$(nproc)" EPA_HYBRID=1; \
    install -m 0755 bin/epa-ng /usr/local/bin/epa-ng-mpi

# optional quick probe; ignore exit if mpirun inside container is constrained
RUN mpirun -n 2 /usr/local/bin/epa-ng-mpi -h >/dev/null 2>&1 || true

# --- Runtime: simple PATH-based entrypoint (no micromamba argv parsing) ---
USER root
RUN printf '%s\n' \
  '#!/usr/bin/env sh' \
  'set -e' \
  'export CONDA_PREFIX=/opt/conda/envs/nanotree' \
  'export PATH="$CONDA_PREFIX/bin:/opt/conda/bin:$PATH"' \
  'exec "$@"' \
  > /usr/local/bin/entry.sh && chmod +x /usr/local/bin/entry.sh

USER mambauser
ENTRYPOINT ["/usr/local/bin/entry.sh"]
CMD ["bash"]
