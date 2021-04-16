#!/usr/bin/env bash

go build -mod=mod -o bin/sa-bot cmd/archivebot/main.go
bin/sa-bot
