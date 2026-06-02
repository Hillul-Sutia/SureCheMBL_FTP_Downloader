FROM python:3.10-bullseye

ENV DEBIAN_FRONTEND=noninteractive

# Install Java and utilities
RUN apt-get update && \
    apt-get install -y \
    default-jdk \
    gcc \
    g++ \
    curl \
    wget \
    procps \
    libxrender1 \
    libxext6 \
    libsm6 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Detect JAVA_HOME automatically
RUN JAVA_PATH=$(dirname $(dirname $(readlink -f $(which javac)))) && \
    echo "JAVA_HOME=$JAVA_PATH" >> /etc/environment

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python packages
# RUN pip install --no-cache-dir \
#     pyspark==3.5.1 \
#     pandas \
#     pyarrow \
#     rdkit

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# COPY . .

ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

CMD ["python", "main.py"]