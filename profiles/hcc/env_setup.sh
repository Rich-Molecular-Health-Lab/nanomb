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
# This matches config.yaml: out_root
export OUT_ROOT="/mnt/nrdstor/richlab/shared/datasets/16s"

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
# Nextflow runtime dirs (host)
# -------------------------------

# ---- Nextflow working/cache dirs (host) ----
export NXF_WORK="${OUT_ROOT}/tmp/wf16s_work"
export NXF_HOME="${OUT_ROOT}/tmp/.nxf"
export NXF_ASSETS="${NXF_HOME}/assets"
mkdir -p "${NXF_WORK}" "${NXF_HOME}" "${NXF_ASSETS}"

# ---- Prefer shared cache on clusters ----
: "${XDG_CACHE_HOME:=${NRDSTOR:-$HOME}/.cache}"
: "${APPTAINER_CACHEDIR:=${NRDSTOR:-$HOME}/.apptainer/cache}"
: "${APPTAINER_TMPDIR:=${NRDSTOR:-$HOME}/.apptainer/tmp}"
mkdir -p "$XDG_CACHE_HOME" "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR"

# Nextflow knows how to use apptainer/singularity cache
export NXF_SINGULARITY_CACHEDIR="$APPTAINER_CACHEDIR"

# Be robust to slow pulls
export NXF_OPTS='-Dsingularity.pullTimeout=1h'

# ---- Pick a container engine automatically ----
if command -v apptainer >/dev/null 2>&1; then
  export NF_PROFILE="apptainer"
elif command -v singularity >/dev/null 2>&1; then
  export NF_PROFILE="singularity"
elif command -v docker >/dev/null 2>&1; then
  export NF_PROFILE="docker"
else
  # last resort: conda (not my first choice, but reproducible if pinned)
  export NF_PROFILE="conda"
fi

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