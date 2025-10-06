# docker/gpu/Dockerfile
FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    MAMBA_ROOT_PREFIX=/opt/conda \
    MAMBA_NO_BANNER=1 \
    CONDA_SUBDIR=linux-64 \
    DORADO_MODELS=/models

RUN apt-get update && apt-get install -y --no-install-recommends \
      bash curl wget git ca-certificates bzip2 xz-utils \
      libgomp1 libstdc++6 \
 && rm -rf /var/lib/apt/lists/*

# micromamba (tar.bz2 -> /opt/bin/micromamba)
RUN curl -fsSL https://micro.mamba.pm/api/micromamba/linux-64/latest \
  | tar -xj -C /opt \
 && ln -s /opt/bin/micromamba /usr/local/bin/micromamba

# Create env from your YAML (installs into /opt/conda/envs/ont)
WORKDIR /opt/env
COPY docker/gpu/environment.gpu.yml .
RUN micromamba create -y -r ${MAMBA_ROOT_PREFIX} -n ont -f environment.gpu.yml \
 && micromamba clean -a -y

# Make the env first on PATH
ENV PATH=${MAMBA_ROOT_PREFIX}/envs/ont/bin:${MAMBA_ROOT_PREFIX}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Standard bind path for models (works for Medaka; Dorado container will bind to the same)
RUN mkdir -p /models
ENV DORADO_MODELS=/models

WORKDIR /workspace
ENTRYPOINT ["/bin/bash"]
