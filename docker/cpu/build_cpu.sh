TAG=0.2

docker build -t aliciamrich/nanomb:$TAG .

docker push aliciamrich/nanomb:$TAG

docker run --rm -it aliciamrich/nanomb:$TAG

------------------------

module load apptainer  
apptainer pull nanomb.sif \
  docker://aliciamrich/nanomb:$TAG
  
apptainer exec nanomb.sif \
  bash -lc 'which isONclust3 isonclust3 vsearch mafft iqtree3 NanoPlot python && \
            isONclust3 --help | head -1 && iqtree3 -v | head -1'