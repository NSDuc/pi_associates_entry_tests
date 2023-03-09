# syntax=docker/dockerfile:1

FROM python:3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV TZ "Asia/Ho_Chi_Minh"

# COPY . .

