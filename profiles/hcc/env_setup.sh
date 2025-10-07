#!/usr/bin/env bash
set -euo pipefail

# -------------------------------
# Respect Swan-provided roots
# -------------------------------
# DO NOT overwrite these; they are set by the cluster environment
: "${WORK:?Swan WORK is not set}"         # e.g. /work/richlab/aliciarich
: "${NRDSTOR:?Swan NRDSTOR is not set}"   # e.g. /mnt/nrdstor/richlab/aliciarich

# -------------------------------
# Project pointers (code + outputs)
# -------------------------------
# Path to the *code repo* (shared, readable by the team)
export PROJ_ROOT="/mnt/nrdstor/richlab/shared/nanomb"

# All data/outputs for this pipeline live under your WORK area:
# This matches config.yaml: out_root: "$WORK/datasets/16s"
export OUT_ROOT="${WORK}/datasets/16s"

# Optional: a scratch area for intermediates under the dataset workspace
export LOCAL_WORK="${OUT_ROOT}/_local_work"

# Ensure key dirs exist
mkdir -p "${OUT_ROOT}" "${LOCAL_WORK}"

# -------------------------------
# Apptainer/Singularity caches
# -------------------------------
# Prefer NRDSTOR-backed cache to avoid filling $HOME or node-local disks
export XDG_CACHE_HOME="${NRDSTOR}/.cache"
export TMPDIR="${NRDSTOR}/tmp"

# Apptainer (preferred names)
export APPTAINER_CACHEDIR="${NRDSTOR}/.apptainer/cache"
export APPTAINER_TMPDIR="${NRDSTOR}/.apptainer/tmp"

# Backward-compat for Singularity
export SINGULARITY_CACHEDIR="${APPTAINER_CACHEDIR}"
export SINGULARITY_TMPDIR="${APPTAINER_TMPDIR}"

mkdir -p "${XDG_CACHE_HOME}" "${TMPDIR}" "${APPTAINER_CACHEDIR}" "${APPTAINER_TMPDIR}"

# -------------------------------
# Echo summary (debugging)
# -------------------------------
cat <<EOF
[env_setup]
WORK                = ${WORK}
NRDSTOR             = ${NRDSTOR}
PROJ_ROOT           = ${PROJ_ROOT}
OUT_ROOT            = ${OUT_ROOT}
LOCAL_WORK          = ${LOCAL_WORK}
APPTAINER_CACHEDIR  = ${APPTAINER_CACHEDIR}
APPTAINER_TMPDIR    = ${APPTAINER_TMPDIR}
EOF