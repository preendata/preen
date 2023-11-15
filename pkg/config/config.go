package config

import (
	"log"
	"os"

	yaml "gopkg.in/yaml.v3"
)

type Connection struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

type Source struct {
	Name       string
	Engine     string
	Connection Connection
}

type Target struct {
	Name       string
	Engine     string
	Connection Connection
}

type Table struct {
	Name string
}

type ReplicationSlotOptions struct {
	Name      string
	Temporary bool
}

type Options struct {
	Plugin                 string
	Publication            string
	ReplicationSlotOptions ReplicationSlotOptions `yaml:"replicationSlotOptions"`
}

type Method struct {
	Name    string
	Options Options
}

type Config struct {
	Sources []Source `yaml:"sources"`
	Tables  []Table  `yaml:"tables"`
	Method  Method   `yaml:"method"`
	Target  Target   `yaml:"target"`
}

func GetConfig() Config {
	file, err := os.ReadFile("plex.yaml")

	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	c := Config{}

	err = yaml.Unmarshal(file, &c)

	if err != nil {
		log.Fatalf("Failed to parse config file: %s", err)
	}

	return c
}
