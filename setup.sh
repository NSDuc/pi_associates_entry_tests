#!/bin/bash

mkdir -p data/kisvn/{log,raw_data,processed_data}
mkdir -p data/vps/log
chmod -R 777 data
cp -Trv sample data

docker build -t pi_associates:syduc -f Dockerfile .
docker-compose restart || docker-compose up -d
