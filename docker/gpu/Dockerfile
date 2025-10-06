FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    MAMBA_ROOT_PREFIX=/opt/conda \
    MAMBA_NO_BANNER=1 \
    CONDA_SUBDIR=linux-64 \
    DORADO_MODELS=/models

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl git bash ca-certificates libgomp1 bzip2 && \
    rm -rf /var/lib/apt/lists/*

# micromamba (tar.bz2)
RUN curl -L https://micro.mamba.pm/api/micromamba/linux-64/latest \
  | tar -xj -C /opt \
 && ln -s /opt/bin/micromamba /usr/local/bin/micromamba

WORKDIR /opt/env
COPY docker/gpu/environment.gpu.yml .
RUN micromamba create -y -r ${MAMBA_ROOT_PREFIX} -n ont -f environment.gpu.yml \
 && micromamba clean -a -y

ENV PATH=${MAMBA_ROOT_PREFIX}/envs/ont/bin:${MAMBA_ROOT_PREFIX}/bin:$PATH

# Dorado GPU binary + consistent model path
ENV DORADO_MODELS=/models
RUN mkdir -p ${DORADO_MODELS}

# Dorado install (correct symlink and clean tarball)
WORKDIR /opt
RUN rm -f /usr/local/bin/dorado \
 && wget -q https://cdn.oxfordnanoportal.com/software/analysis/dorado-1.1.1-linux-x64.tar.gz \
 && tar -xzf dorado-1.1.1-linux-x64.tar.gz -C /opt \
 && ln -sf /opt/dorado-1.1.1-linux-x64/bin/dorado /usr/local/bin/dorado \
 && rm -f dorado-1.1.1-linux-x64.tar.gz

# Make both the conda env AND /usr/local/bin visible
ENV PATH=/opt/conda/envs/ont/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR /workspace
# COPY workflow/ /workspace/workflow/

ENTRYPOINT ["/bin/bash"]
