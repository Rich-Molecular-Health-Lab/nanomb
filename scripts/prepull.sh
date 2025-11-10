#!/bin/bash
#SBATCH --job-name=prepull
#SBATCH --account=richlab
#SBATCH --partition=batch,guest
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G           
#SBATCH --time=01:00:00
#SBATCH --output=prepull.%j.out
#SBATCH --error=prepull.%j.err

module load apptainer

# --- Run from repo root ---
cd /mnt/nrdstor/richlab/shared/nanomb || {
  echo "ERROR: repo path not found"; exit 1;
}

mkdir -p containers

cd containers

# pull all images you referenced in config.yaml
apptainer pull dorado.sif docker://nanoporetech/dorado:latest
apptainer pull nextflow.sif docker://nextflow/nextflow:25.10.0
apptainer pull nanomb.sif docker://aliciamrich/nanomb-cpu:2025-10-11
apptainer pull nanombgpu.sif docker://aliciamrich/nanombgpu:0.2.0-gpu
apptainer pull nanoalign.sif docker://aliciamrich/nanoalign:cpu

echo "prepull complete"