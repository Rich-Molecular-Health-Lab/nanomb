#!/bin/bash
#SBATCH --job-name=epa
#SBATCH --account=richlab
#SBATCH --partition=batch
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=7-00:00:00
#SBATCH --output=nanomb_epa_%j.out

# activate environment
module load anaconda
module load apptainer
conda activate "$NRDSTOR/snakemake"

# --- Run from repo root ---
cd /mnt/nrdstor/richlab/shared/nanomb || {
  echo "ERROR: repo path not found"; exit 1;
}

mkdir -p /mnt/nrdstor/richlab/shared/nanomb/.snakemake/slurm_logs

rm -rf /mnt/nrdstor/richlab/shared/datasets/16s/loris/culi/otu/epa_tmp

# --- Prepare environment variables for the workflow ---
source profiles/hcc/env_setup.sh

# --- Launch the workflow ---
snakemake \
  --profile profiles/hcc -j 16 \
  --rerun-triggers mtime \
  --rerun-triggers code \
  --rerun-triggers params \
  /mnt/nrdstor/richlab/shared/datasets/16s/loris/culi/otu/unknowns.placements.jplace