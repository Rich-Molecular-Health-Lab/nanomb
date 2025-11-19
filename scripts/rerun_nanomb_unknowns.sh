#!/bin/bash
#SBATCH --job-name=nanomb_unkn
#SBATCH --account=richlab
#SBATCH --partition=batch
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=7-00:00:00
#SBATCH --output=nanomb_unkn_%j.out

# activate environment
module load anaconda
module load apptainer
conda activate "$NRDSTOR/snakemake"

# --- Run from repo root ---
cd /mnt/nrdstor/richlab/shared/nanomb || {
  echo "ERROR: repo path not found"; exit 1;
}

mkdir -p /mnt/nrdstor/richlab/shared/nanomb/.snakemake/slurm_logs

# --- Prepare environment variables for the workflow ---
source profiles/hcc/env_setup.sh
DSET=/mnt/nrdstor/richlab/shared/datasets/16s/loris/culi

# --- Launch the workflow ---
snakemake \
  "$DSET/otu/otu_tree.with_unknowns.nwk" \
  --profile profiles/hcc \
  --rerun-incomplete \
  --rerun-triggers mtime \
  --restart-times 2 \
  --keep-going \
  --printshellcmds 