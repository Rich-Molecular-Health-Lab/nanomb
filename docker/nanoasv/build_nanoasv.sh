
git submodule add https://github.com/ImagoXV/NanoASV.git third_party/nanoasv
git submodule update --init --recursive


docker build -f docker/nanoasv/Dockerfile -t aliciamrich/nanoasv:0.2.0-cpu .

# sanity checks
docker run --rm -it aliciamrich/nanoasv:0.2.0-cpu -lc '
which snakemake; snakemake --version;
vsearch --version | head -1;
minimap2 --version;
'



docker run --rm -it aliciamrich/nanoasv:0.2.0-cpu
# you're now inside bash (ENTRYPOINT)
uname -m
nanoasv --version

# if these work then run

docker push aliciamrich/nanoasv:0.2.0-cpu
