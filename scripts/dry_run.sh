srun --mem=2gb --nodes=1 --ntasks-per-node=1 --pty $SHELL

# activate environment
module load anaconda
conda activate "$NRDSTOR/snakemake"

# --- Run from repo root ---
cd /mnt/nrdstor/richlab/shared/nanomb 

# --- Prepare environment variables for the workflow ---
source profiles/hcc/env_setup.sh

snakemake --profile profiles/hcc -n -p --rerun-triggers params --summary

snakemake --profile profiles/hcc -n --dag | dot -Tsvg > dag.svg

snakemake --profile profiles/hcc -n -p \
  --rerun-triggers params \
  --list-input-changes \
  --list-params-changes \
  --list-changes code