FROM mambaorg/micromamba:1.5.9

ENV DEBIAN_FRONTEND=noninteractive \
    MAMBA_NO_BANNER=1 \
    CONDA_SUBDIR=linux-64 \
    MAMBA_ROOT_PREFIX=/opt/conda \
    MAMBA_DOCKERFILE_ACTIVATE=1 \
    MAMBA_USER=root

# run as root (simplest + avoids missing user issues)
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      wget curl ca-certificates pigz gzip \
 && rm -rf /var/lib/apt/lists/*

# ---- Embed NanoASV workflow (adjust ARG if your path differs) ----
ARG NANOASV_DIR=third_party/nanoasv
WORKDIR /workspace
COPY ${NANOASV_DIR}/ /workspace/workflow/
ENV NANOASV_WORKFLOW_DIR=/workspace/workflow

# ---- Conda env ----
WORKDIR /opt/env
COPY docker/nanoasv/environment.nanoasv.yml .
RUN micromamba install -y -n base -f environment.nanoasv.yml \
 && micromamba clean -a -y

# make sure /usr/local/bin is visible
ENV PATH=/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Stable bind path for reference DBs (e.g., SILVA)
ENV NANOASV_DB=/refdb
RUN mkdir -p ${NANOASV_DB}

# ---- Friendly CLI wrapper so "nanoasv ..." just works ----
RUN printf '%s\n' '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'WF_DIR="${NANOASV_WORKFLOW_DIR:-/workspace/workflow}"' \
  'SNAKE="${NANOASV_SNAKEFILE:-$WF_DIR/Snakefile}"' \
  'if [[ ! -f "$SNAKE" ]]; then' \
  '  CAND="$(find "$WF_DIR" -maxdepth 3 -iname Snakefile -print -quit 2>/dev/null || true)"' \
  '  [[ -n "${CAND:-}" ]] && SNAKE="$CAND"' \
  'fi' \
  'if [[ ! -f "$SNAKE" ]]; then' \
  '  echo "Snakefile not found under $WF_DIR. Set NANOASV_WORKFLOW_DIR or mount one." >&2; exit 1' \
  'fi' \
  'cd "$(dirname "$SNAKE")"' \
  'exec snakemake -s "$SNAKE" "$@"' \
  > /usr/local/bin/nanoasv && chmod +x /usr/local/bin/nanoasv

# Default to the wrapper; override with --entrypoint bash if you want a shell
ENTRYPOINT ["/usr/local/bin/nanoasv"]
CMD ["-h"]
