docker build -f docker/gpu/Dockerfile -t aliciamrich/nanombgpu:0.2.0-gpu .

docker build --no-cache -f docker/gpu/Dockerfile -t aliciamrich/nanombgpu:0.2.0-gpu .

docker build --no-cache -f docker/gpu/Dockerfile -t aliciamrich/nanombgpu:0.2.0-gpu .

docker run --rm -it aliciamrich/nanombgpu:0.2.0-gpu -lc '
which dorado && dorado --version;
python - <<PY
import torch
print("torch", torch.__version__)
print("cuda available:", torch.cuda.is_available(), "devices:", torch.cuda.device_count())
PY'

docker run --rm -it aliciamrich/nanombgpu:0.2.0-gpu -lc '
ls -l /usr/local/bin/dorado;
dorado --version;
'

# sanity checks
docker run --rm -it aliciamrich/nanombgpu:0.2.0-gpu
# you're now inside bash (ENTRYPOINT)
uname -m
dorado --version
medaka --version

# if these work then run

docker push aliciamrich/nanombgpu:0.2.0-gpu
