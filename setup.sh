#!/bin/bash

mkdir -p data/kisvn/{log_data,raw_data,processed_data}
mkdir -p data/vps/{log_data,raw_data,processed_data}
cp -r sample/. data/.

docker build -t pi_associates:syduc -f Dockerfile .
docker-compose up -d
