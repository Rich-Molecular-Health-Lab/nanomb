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
conda activate "$NRDSTOR/snakemake"

# --- Run from repo root ---
cd /mnt/nrdstor/richlab/shared/nanomb || {
  echo "ERROR: repo path not found"; exit 1;
}

# --- Prepare environment variables for the workflow ---
source profiles/hcc/env_setup.sh

# --- Launch the workflow ---
snakemake \
  --profile profiles/hcc \
  --rerun-incomplete \
  --keep-going \
  --printshellcmds