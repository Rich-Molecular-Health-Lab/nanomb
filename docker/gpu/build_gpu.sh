TAG=0.3

docker build --no-cache -f docker/gpu/Dockerfile -t aliciamrich/nanombgpu:$TAG .

docker push aliciamrich/nanombgpu:$TAG

docker run --rm -it aliciamrich/nanombgpu:$TAG
