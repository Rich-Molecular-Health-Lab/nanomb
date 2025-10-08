docker build -t aliciamrich/nanoalign:cpu .

# sanity checks
docker run --rm -it aliciamrich/nanoalign:cpu 

# you're now inside bash (ENTRYPOINT)
uname -m
minimap2 --help | head -n 3
samtools --version
racon --version


# if these work then run

docker push aliciamrich/nanoalign:cpu
