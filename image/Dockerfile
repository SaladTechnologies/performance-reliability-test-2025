FROM nvidia/cuda:12.8.1-cudnn-devel-ubuntu22.04

RUN apt-get update && apt-get install -y \
    wget \
    tar \
    tree \
    nano \
    curl \
    net-tools \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install VS Code Server for remote debugging
# https://docs.salad.com/container-engine/tutorials/development-tools/vscode-remote-development#interactive-mode
RUN curl -Lk 'https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64' -o vscode_cli.tar.gz && \
    tar -xf vscode_cli.tar.gz && \
    mv code /usr/local/bin/code && \
    rm vscode_cli.tar.gz

# To connect using VS Code:
# Log in the instance using the terminal, and then run the following commands:
# code tunnel user login --provider github
# nohup code tunnel --accept-server-license-terms --name 001 &> output.log &

RUN pip install --upgrade pip
RUN pip install python-dotenv speedtest-cli pythonping requests boto3 psutil 
RUN pip install tzdata

WORKDIR /app
COPY lolMiner helper.py main.py /app/
RUN chmod +x /app/lolMiner

ENV CUDA_VISIBLE_DEVICES=0

CMD ["python3", "./main.py"]

# Image: docker.io/saladtechnologies/misc:001-performance-test 