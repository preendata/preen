FROM golang:1.21.5-bullseye

COPY . /preen

WORKDIR /preen

RUN go mod tidy
