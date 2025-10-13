#!/bin/bash
#SBATCH --job-name=basecall_hdz15
#SBATCH --account=richlab
#SBATCH --partition=batch
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=7-00:00:00
#SBATCH --output=basecall_hdz15_%j.out

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
snakemake --profile profiles/hcc --unlock

# --- Launch the workflow ---
snakemake --profile profiles/hcc -j 8 --rerun-incomplete \
  "$WORK/datasets/16s/loris/basecalled/hdz15/hdz15.bam" \
  "$WORK/datasets/16s/loris/dorado_summaries/hdz15/basecall/hdz15_basecall_summary.tsv"
  