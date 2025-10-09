# 1️⃣ Build the container (with IQ-TREE 3)
docker build \
  -t nanomb:0.2.0-cpu \
  -f docker/cpu/Dockerfile .

# optional: sanity check before tagging/pushing
docker run --burm -it nanomb:iqtree3 bash -lc "iqtree -h | head -1; isonclust3 --help | head -1"


docker tag nanomb:iqtree3 aliciamrich/nanomb:0.2.0-cpu
docker push aliciamrich/nanomb:0.2.0-cpu