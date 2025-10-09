# docker/cpu/Dockerfile
FROM mambaorg/micromamba:1.5.9
ENV DEBIAN_FRONTEND=noninteractive

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git pigz gzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/env
COPY docker/cpu/environment.cpu.yml .
RUN micromamba create -y -n nanomb -f environment.cpu.yml && micromamba clean -a -y
ENV PATH=/opt/conda/envs/nanomb/bin:/opt/conda/bin:$PATH

WORKDIR /workspace
ENTRYPOINT ["/bin/bash"]