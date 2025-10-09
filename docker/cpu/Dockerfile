# docker/cpu/Dockerfile
FROM mambaorg/micromamba:1.5.9
ENV DEBIAN_FRONTEND=noninteractive MAMBA_DOCKERFILE_ACTIVATE=1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git pigz gzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/env
COPY docker/cpu/environment.cpu.yml .
RUN micromamba install -y -n base -f environment.cpu.yml && micromamba clean -a -y
ENV PATH=/opt/conda/bin:$PATH

WORKDIR /workspace
ENTRYPOINT ["/bin/bash"]