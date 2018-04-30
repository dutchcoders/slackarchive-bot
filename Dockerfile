FROM golang:latest AS builder

ADD . /go/src/github.com/dutchcoders/slackarchive-bot
WORKDIR /go/src/github.com/dutchcoders/slackarchive-bot

ARG LDFLAGS=""
RUN go build -tags="" -o /go/bin/app github.com/dutchcoders/slackarchive-bot/cmd/archivebot/

FROM debian
RUN apt-get update && apt-get install -y ca-certificates netcat
COPY --from=builder /go/bin/app /slackarchive/archivebot

RUN mkdir /config 

ENTRYPOINT ["/slackarchive/archivebot", "--config", "/config/config.yaml"]
