# Choose a CUDA runtime that matches your cluster driver (adjust if needed)
FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

ARG MEDAKA_VERSION=2.1.1
RUN pip3 install --no-cache-dir "medaka==${MEDAKA_VERSION}"

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

RUN ln -s /usr/bin/python3 /usr/local/bin/python || true