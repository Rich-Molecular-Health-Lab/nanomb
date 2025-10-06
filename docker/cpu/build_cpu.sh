docker build -f docker/cpu/Dockerfile -t aliciamrich/nanomb:0.2.0-cpu .

# sanity checks
docker run --rm -it aliciamrich/nanomb:0.2.0-cpu -lc '
uname -m
which isONclust3 && file "$(which isONclust3)" || true
which fastcat && file "$(which fastcat)" || true
isONclust3 --help | head -n 3 || true
fastcat --version || fastcat --help | head -n 3 || true
NanoPlot --version
'
docker run --rm -it aliciamrich/nanomb:0.2.0-cpu
# you're now inside bash (ENTRYPOINT)
uname -m
isONclust3 --help | head -n 3
fastcat --version
NanoPlot --version


# if these work then run

docker push aliciamrich/nanomb:0.2.0-cpu
