#!/bin/bash
go get -u
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags netgo -ldflags '-w -extldflags "-static"'
