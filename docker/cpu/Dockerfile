# ---- Stage 1: build isONclust3 (Rust) ----
FROM rust:1-bullseye AS iso3build
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git pkg-config clang && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 https://github.com/aljpetri/isONclust3.git .
# Build & install artifact into /opt/iso3 to copy later
RUN cargo build --release --locked && \
    install -Dm755 target/release/isONclust3 /opt/iso3/bin/isONclust3

# ---- Stage 2: runtime image with micromamba env ----
FROM mambaorg/micromamba:1.5.9
ENV DEBIAN_FRONTEND=noninteractive MAMBA_DOCKERFILE_ACTIVATE=1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git pigz gzip ca-certificates curl xz-utils libgomp1 \
  && rm -rf /var/lib/apt/lists/*

# ----- conda env (lean toolset you need from conda) -----
WORKDIR /opt/env
COPY docker/cpu/environment.cpu.yml .
# Tip: if memory is tight during solve, pin exact versions in environment.cpu.yml
# create env (keep what you had)
RUN micromamba create -y -n nanomb -c conda-forge -c bioconda -c nanoporetech -c hcc -c defaults \
      python=3.11 snakemake-minimal=8.11 vsearch racon spoa pysam nanoporetech::fastcat \
  && micromamba clean -a -y

# add the missing bits
RUN micromamba install -y -n nanomb -c conda-forge -c bioconda \
      mafft nanoplot \
  && micromamba clean -a -y

ENV MAMBA_DEFAULT_ENV=nanomb
ENV PATH=/opt/conda/envs/nanomb/bin:/opt/conda/bin:$PATH
# ---- isONclust3 (from the Rust build stage) ----
COPY --from=iso3build /opt/iso3/bin/isONclust3 /usr/local/bin/isonclust3

# --- IQ-TREE (prebuilt tarball) ---
ARG IQTREE_URL
RUN test -n "$IQTREE_URL" || (echo "ERROR: Set --build-arg IQTREE_URL=<download-url>"; exit 1)
RUN set -euo pipefail; cd /tmp; \
  curl -L -o iqtree.tar.xz "$IQTREE_URL"; \
  topdir="$(tar -tf iqtree.tar.xz | head -1 | cut -d/ -f1)"; \
  mkdir -p /opt && tar -xJf iqtree.tar.xz -C /opt; \
  if [ -x "/opt/${topdir}/bin/iqtree3" ]; then \
      ln -sf "/opt/${topdir}/bin/iqtree3" /usr/local/bin/iqtree3; \
  elif [ -x "/opt/${topdir}/iqtree3" ]; then \
      ln -sf "/opt/${topdir}/iqtree3" /usr/local/bin/iqtree3; \
  else \
      tgt="$(find /opt/${topdir} -maxdepth 2 -type f -name iqtree3 | head -n1)"; \
      test -n "$tgt" && ln -sf "$tgt" /usr/local/bin/iqtree3; \
  fi; \
  ln -sf /usr/local/bin/iqtree3 /usr/local/bin/iqtree; \
  rm -f /usr/local/bin/iqtree3_intel || true; \
  rm -f iqtree.tar.xz
  
  
# ---- sanity banner on container start ----
CMD ["bash", "-lc", "echo 'Tools on PATH:'; \
  which vsearch spoa mafft iqtree nanoplot python isonclust3 || true; \
  echo 'Versions:'; \
  vsearch --version | head -1 || true; \
  spoa --version || true; \
  mafft --version | head -1 || true; \
  iqtree -h | head -1 || true; \
  nanoplot --version || true; \
  python --version || true; \
  isonclust3 --help | head -1 || true; \
  bash"]