snakemake -j 32 --use-singularity \
  --singularity-args "--nv -B /work -B /lustre -B /mnt/nrdstor -B /home -B /models:/models -B /refdb:/refdb"
