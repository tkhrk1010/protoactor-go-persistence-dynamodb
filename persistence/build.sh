#!bin/sh

cd $(dirname "$0") && pwd

protoc --go_out=. --go_opt=paths=source_relative \
    -I../../ -I. protos.proto
