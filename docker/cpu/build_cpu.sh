docker build \
  --build-arg IQTREE_URL="https://github.com/iqtree/iqtree3/releases/download/v3.0.1/iqtree-3.0.1-Linux.tar.gz" \
  -t nanomb:0.2.0-cpu \
  -f docker/cpu/Dockerfile .
  
# optional: sanity check before tagging/pushing
docker run --burm -it nanomb:iqtree3 bash -lc "iqtree -h | head -1; isonclust3 --help | head -1"


docker tag nanomb:iqtree3 aliciamrich/nanomb:0.2.0-cpu
docker push aliciamrich/nanomb:0.2.0-cpu