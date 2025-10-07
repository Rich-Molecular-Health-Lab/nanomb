# activate environment
module load anaconda
conda activate "$NRDSTOR/snakemake"

# --- Run from repo root ---
cd /mnt/nrdstor/richlab/shared/nanomb 

# --- Prepare environment variables for the workflow ---
source profiles/hcc/env_setup.sh

snakemake -n -p --profile profiles/hcc