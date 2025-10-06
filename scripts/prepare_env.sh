# pick your NRDSTOR base
export NRD=/mnt/nrdstor/richlab/aliciarich

# make dirs
mkdir -p $NRD/.cache $NRD/tmp $NRD/.apptainer/cache $NRD/.apptainer/tmp

# 1) generic caches & tmp (Snakemake/PlatformDirs honors XDG_CACHE_HOME)
export XDG_CACHE_HOME=$NRD/.cache
export TMPDIR=$NRD/tmp

# 2) apptainer/singularity caches & tmp (for containers on compute nodes too)
export APPTAINER_CACHEDIR=$NRD/.apptainer/cache
export APPTAINER_TMPDIR=$NRD/.apptainer/tmp
export SINGULARITY_CACHEDIR=$APPTAINER_CACHEDIR
export SINGULARITY_TMPDIR=$APPTAINER_TMPDIR

# optional: free tiny-home space
rm -rf ~/.cache/snakemake ~/.apptainer ~/.singularity 2>/dev/null || true