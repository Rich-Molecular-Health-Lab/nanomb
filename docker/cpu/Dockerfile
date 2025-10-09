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
RUN micromamba create -y -n nanomb -f environment.cpu.yml && micromamba clean -a -y

ENV MAMBA_DEFAULT_ENV=nanomb
ENV PATH=/opt/conda/envs/nanomb/bin:/opt/conda/bin:$PATH

# ---- isONclust3 (from the Rust build stage) ----
COPY --from=iso3build /opt/iso3/bin/isONclust3 /usr/local/bin/isonclust3

# ---- IQ-TREE 3 (prebuilt binary) ----
# Provide the exact URL for the Linux release you want.
# Examples (these are examples—use the one you need):
#  - Tarball: https://github.com/iqtree/iqtree3/releases/download/v3.x.y/iqtree-3.x.y-Linux.tar.xz
#  - AppImage: https://github.com/iqtree/iqtree3/releases/download/v3.x.y/iqtree-3.x.y-Linux-x86_64.AppImage
#
# Use a tarball if available; if you use an AppImage, mark it executable and link it to "iqtree".
ARG IQTREE_URL=
RUN test -n "$IQTREE_URL" || (echo "ERROR: Set --build-arg IQTREE_URL=<download-url>"; exit 1)

# If it's a tar.xz (preferred):
RUN case "$IQTREE_URL" in \
      *.tar.xz|*.tar.gz) \
        mkdir -p /usr/local/iqtree && \
        curl -L "$IQTREE_URL" -o /tmp/iqtree.tar && \
        tar -xf /tmp/iqtree.tar -C /usr/local/iqtree --strip-components=1 && \
        ln -sf /usr/local/iqtree/bin/iqtree3 /usr/local/bin/iqtree && \
        rm -f /tmp/iqtree.tar \
      ;; \
      *.AppImage) \
        curl -L "$IQTREE_URL" -o /usr/local/bin/iqtree3 && \
        chmod +x /usr/local/bin/iqtree3 && \
        ln -sf /usr/local/bin/iqtree3 /usr/local/bin/iqtree \
      ;; \
      *) echo "Unknown IQTREE_URL artifact type"; exit 2 ;; \
    esac

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