FROM golang:1.17.3-buster

RUN apt-get update && apt-get install -y python3-pip libcurl4-nss-dev

# Set the working directory to /app
WORKDIR /root/app
ENV PYTHONPATH="/root/app"
ENV GOPATH="/root/go"

# Add the python requirements first in order to docker cache them
ADD ./requirements.txt /root/app/requirements.txt

# Upgrade setuptools in order to install latest chirpstack_api dependencies
RUN pip3 install --upgrade pip setuptools

# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

# Copy the current directory contents into the container at /app
ADD . /root/app/

# Install go dependencies
RUN go env -w GO111MODULE=off
RUN go get -d ./...

# Compile go library
WORKDIR /root/app/lorawanwrapper/utils
RUN go build -o lorawanWrapper.so -buildmode=c-shared jsonUnmarshaler.go lorawanWrapper.go micGenerator.go sessionKeysGenerator.go

WORKDIR /root/app
CMD python3 auditing/datacollectors/Orchestrator.py