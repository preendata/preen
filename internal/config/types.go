package config

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
	Name    string
	Schema  string
	Columns []string
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
