#!/bin/bash

mkdir -p ./data/kisvn/raw_data
mkdir -p ./data/kisvn/processed_data
cp -r ./sampe/* ./data/*

docker pull python:3
docker pull confluentinc/cp-kafka:7.3.2
docker pull confluentinc/cp-zookeeper:7.3.2

docker build -t pi_associates:syduc -f Dockerfile .
