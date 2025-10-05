export SIFROOT=/mnt/nrdstor/richlab/shared/containers
export APPTAINER_TMPDIR=/work/richlab/$USER/tmp/apptainer-tmp
export APPTAINER_CACHEDIR=/work/richlab/$USER/.apptainer/cache
mkdir -p "$SIFROOT" "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"

srun --mem=8gb --nodes=1 --ntasks-per-node=4 --pty $SHELL


module load apptainer

apptainer build --fakeroot \
  $SIFROOT/16s-cpu_2025.10.05.sif 16s-cpu.def

chmod 644 $SIFROOT/16s-cpu_2025.10.05.sif