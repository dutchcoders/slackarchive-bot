package main

import (
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	archivebot "github.com/dutchcoders/slackarchive-bot"
	config "github.com/dutchcoders/slackarchive-bot/config"

	"github.com/codegangsta/cli"
	_ "github.com/go-sql-driver/mysql"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

var version string = "0.1"

func main() {
	app := cli.NewApp()
	app.Name = "SlackArchive bot"
	app.Version = version
	app.Flags = append(app.Flags, []cli.Flag{
		cli.StringFlag{
			Name:   "config, c",
			Value:  "config.yaml",
			Usage:  "Custom configuration file path",
			EnvVar: "",
		},
	}...)

	app.Before = config.Load
	app.Action = run

	app.Run(os.Args)
}

func run(c *cli.Context) {
	conf := config.Get()

	sa := archivebot.New(conf)
	if sa == nil {
		return
	}

	sa.Start()

	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt)
	signal.Notify(s, syscall.SIGTERM)

	for {
		switch <-s {
		case os.Interrupt:
			return
		case syscall.SIGTERM:
			return
		case syscall.SIGUSR1:
			sa.Reload()
		}
	}
}
