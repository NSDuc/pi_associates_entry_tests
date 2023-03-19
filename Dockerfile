# syntax=docker/dockerfile:1

FROM python:3
ARG EXEC_USER=syduc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -ms /bin/bash ${EXEC_USER}
USER ${EXEC_USER}
ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV TZ "Asia/Ho_Chi_Minh"

WORKDIR /app

# COPY . .

