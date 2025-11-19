#!/bin/bash
#SBATCH --job-name=epa
#SBATCH --account=richlab
#SBATCH --partition=batch
#SBATCH --cpus-per-task=16          # matches rule epa_place_unknowns.threads
#SBATCH --mem=32G                   # adjust to Rq("epa_place_unknowns", "mem_mb")
#SBATCH --time=3-00:00:00
#SBATCH --output=nanomb_epa_%j.out

# --- Activate environment ---
module load anaconda
module load apptainer
conda activate "$NRDSTOR/snakemake"

# --- Navigate to repo root ---
cd /mnt/nrdstor/richlab/shared/nanomb || {
  echo "ERROR: repo path not found"; exit 1;
}

mkdir -p /mnt/nrdstor/richlab/shared/nanomb/.snakemake/slurm_logs

# --- Prepare environment variables ---
source profiles/hcc/env_setup.sh

# --- Path to your dataset output (for clarity) ---
DSET=/mnt/nrdstor/richlab/shared/datasets/16s/loris/culi

# --- Run just the EPA placement rule ---
snakemake \
  --profile profiles/hcc \
  --cores 16 \
  --rerun-incomplete \
  --rerun-triggers mtime \
  --restart-times 1 \
  --keep-going \
  --printshellcmds \
  --jobs 1 \
  "$DSET/otu/unknowns.placements.jplace"