# containers/biopython-sync/Dockerfile
# Minimal, reproducible Biopython environment for the sync_exports_for_R rule
FROM python:3.11-slim

# Environment settings
ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install dependencies (pinned versions for reproducibility)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates build-essential && \
    python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir "numpy==1.26.4" "biopython==1.83" && \
    apt-get purge -y build-essential && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Default to Python entrypoint for simplicity
ENTRYPOINT ["python3"]