FROM mambaorg/micromamba:2.3.2-0
USER mambauser
COPY --chown=mambauser:mambauser environment.yml /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml && micromamba clean -a -y
ENV PATH=/opt/conda/bin:$PATH