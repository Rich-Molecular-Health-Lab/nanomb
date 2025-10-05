FROM mambaorg/micromamba:2.3.2-0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends tini procps \
    && rm -rf /var/lib/apt/lists/*
USER mambauser

# Copy the conda env spec and install
COPY --chown=mambauser:mambauser environment.yml /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml && micromamba clean -a -y
ENV PATH=/opt/conda/bin:$PATH

# Install isONclust3 via pip from a pinned ref (override at build-time)
ARG ISONCLUST3_REF=main
RUN pip install --no-cache-dir "git+https://github.com/ksahlin/isONclust3@${ISONCLUST3_REF}"

ENTRYPOINT ["/usr/bin/tini", "--"]