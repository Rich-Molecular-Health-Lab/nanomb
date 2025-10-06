#!/bin/bash
#SBATCH --job-name=nanomb
#SBATCH --account=richlab
#SBATCH --partition=batch
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=7-00:00:00
#SBATCH --output=nanomb_snakemake_%j.out

# activate environment
module load anaconda
conda activate $NRDSTOR/snakemake

# safety: run from your repo root
cd /mnt/nrdstor/richlab/shared/nanomb

# export cache/temp dirs to NRDSTOR
export XDG_CACHE_HOME=/mnt/nrdstor/richlab/aliciarich/.cache
export TMPDIR=/mnt/nrdstor/richlab/aliciarich/tmp
export APPTAINER_CACHEDIR=/mnt/nrdstor/richlab/aliciarich/.apptainer/cache
export APPTAINER_TMPDIR=/mnt/nrdstor/richlab/aliciarich/.apptainer/tmp

# main call
snakemake --profile profiles/hcc \
  --executor slurm --mode default --jobs 16 \
  --config pod5_in=/work/richlab/$USER/datasets/16s/loris/pod5 \
  --rerun-incomplete --printshellcmds