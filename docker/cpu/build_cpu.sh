docker build \
  --build-arg IQTREE_URL="https://github.com/iqtree/iqtree3/releases/download/v3.0.1/iqtree-3.0.1-Linux.tar.gz" \
  -t nanomb:0.2.0-cpu \
  -f docker/cpu/Dockerfile .
  

docker push aliciamrich/nanomb:0.2.0-cpu

docker run --rm -it nanomb:0.2.0-cpu bash -lc '
  echo "PATH=$PATH";
  which python mafft NanoPlot iqtree isonclust3;
  python --version;
  mafft --version | head -1;
  NanoPlot --version;
  iqtree -h | head -1;
  isonclust3 --help | head -1;
'
