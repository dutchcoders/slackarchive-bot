package config

import (
	"fmt"
	"io/ioutil"

	"github.com/codegangsta/cli"
	"github.com/imdario/mergo"

	yaml "gopkg.in/yaml.v2"
)

var config Config = DefaultConfig

func Load(c *cli.Context) error {
	var err error
	if err = config.Load(c.GlobalString("config")); err != nil {
		fmt.Println("Error loading config %s", err.Error())
		return err
	}

	// initialize
	return mergo.MergeWithOverwrite(&config, &Config{
		DSN: c.GlobalString("dsn"),
	})
}

func Get() *Config {
	return &config
}

type Config struct {
	DSN string `yaml:"dsn"`

	ApiURL   string `yaml:"api-url"`
	ApiToken string `yaml:"api-token"`

	DataDir string `yaml:"data-dir"`
	Name    string `yaml:"name"`

	ElasticSearch struct {
		Host string `yaml:"host"`
	} `yaml:"elasticsearch"`

	Tokens []string `yaml:"tokens"`
}

var DefaultConfig Config = Config{}

func (c *Config) Load(path string) error {
	var data []byte
	var err error
	if data, err = ioutil.ReadFile(path); err != nil {
		return err
	}

	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}

	tmp := Config{
		DataDir: tmpDir,
	}

	if err = yaml.Unmarshal([]byte(data), &tmp); err != nil {
		return err
	}

	return mergo.MergeWithOverwrite(c, tmp)
}
