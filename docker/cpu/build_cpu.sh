# 1) Pick a versioned tag (date or git sha)
TAG=2025-10-11
IQURL="https://github.com/iqtree/iqtree3/releases/download/v3.0.1/iqtree-3.0.1-Linux.tar.gz"

# 2) Build for the cluster arch (most HCC nodes are x86_64)
docker build \
  --platform linux/amd64 \
  --build-arg IQTREE_URL="$IQURL" \
  -t <dockerhub-user>/nanomb-cpu:$TAG \
  -f Dockerfile.cpu .

# 3) Push to Docker Hub
docker push <dockerhub-user>/nanomb-cpu:$TAG

------------------------

module load apptainer  # if needed on HCC
apptainer pull /mnt/nrdstor/richlab/shared/containers/nanomb.sif \
  docker://docker.io/<dockerhub-user>/nanomb-cpu:$TAG
  
apptainer exec /mnt/nrdstor/richlab/shared/containers/nanomb.sif \
  bash -lc 'which isONclust3 isonclust3 vsearch mafft iqtree3 NanoPlot python && \
            isONclust3 --help | head -1 && iqtree3 -v | head -1'