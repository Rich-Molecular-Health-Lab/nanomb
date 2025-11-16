TAG=0.5

docker build -t aliciamrich/nanotree:$TAG .

docker push aliciamrich/nanotree:$TAG

docker run --rm -it aliciamrich/nanotree:$TAG
