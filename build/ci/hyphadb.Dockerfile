FROM golang:1.21.5-bullseye

COPY . /hyphadb

WORKDIR /hyphadb

RUN go mod tidy
