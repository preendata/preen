package config

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"reflect"
	"regexp"

	"github.com/joho/godotenv"
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

var envRegex = regexp.MustCompile(`^\${(\w+)}$`)

func init() {
	godotenv.Load()
}

func GetConfig(filenames ...string) Config {
	filename := "plex.yaml"
	if len(filenames) > 0 {
		filename = filenames[0]
	}

	file, err := os.ReadFile(filename)

	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	c := Config{}

	err = yaml.Unmarshal(file, &c)

	if err != nil {
		log.Fatalf("Failed to parse config file: %s", err)
	}

	fromEnv(&c)

	return c
}

func AddSource(filename string, newSource Source) error {
	// Read the YAML file
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("could not read file: %v", err)
	}

	// Unmarshal the YAML into a Config struct
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return fmt.Errorf("could not unmarshal YAML: %v", err)
	}

	for _, source := range config.Sources {
		err := validateNewSource(source, newSource)
		if err != nil {
			return err
		}
	}

	// Add the new source
	config.Sources = append(config.Sources, newSource)

	// Marshal the updated config back to YAML
	updatedData, err := yaml.Marshal(&config)
	if err != nil {
		return fmt.Errorf("could not marshal YAML: %v", err)
	}

	// Write the updated YAML back to the file
	err = os.WriteFile(filename, updatedData, 0644)
	if err != nil {
		return fmt.Errorf("could not write file: %v", err)
	}

	return nil
}

func validateNewSource(existingSource Source, newSource Source) error {
	if existingSource.Name == newSource.Name {
		return fmt.Errorf("a database source already exists with the name: %s", newSource.Name)
	}

	if existingSource.Connection.Host == newSource.Connection.Host &&
		existingSource.Connection.Database == newSource.Connection.Database &&
		existingSource.Connection.Port == newSource.Connection.Port {
		return fmt.Errorf("a connection with the same combination of host: %s, port: %d, and database: %s already exists", newSource.Connection.Host, newSource.Connection.Port, newSource.Connection.Database)
	}

	return nil
}

func fromEnv(v interface{}) {
	_fromEnv(reflect.ValueOf(v).Elem()) // assumes pointer to struct
}

// recursive
func _fromEnv(rv reflect.Value) {
	for i := 0; i < rv.NumField(); i++ {
		fv := rv.Field(i)
		if fv.Kind() == reflect.Ptr {
			fv = fv.Elem()
		}
		if fv.Kind() == reflect.Struct {
			_fromEnv(fv)
			continue
		}
		if fv.Kind() == reflect.Slice {
			for j := 0; j < fv.Len(); j++ {
				if fv.Index(j).Kind() == reflect.String {
					match := envRegex.FindStringSubmatch(fv.Index(j).String())
					if len(match) > 1 {
						slog.Debug(
							fmt.Sprintf("Setting env var: '%s'", match[1]),
						)
						fv.SetString(os.Getenv(match[1]))
					}
				}
				if fv.Index(j).Kind() == reflect.Struct {
					_fromEnv(fv.Index(j))
					continue
				}
			}
		}
		if fv.Kind() == reflect.String {
			match := envRegex.FindStringSubmatch(fv.String())
			if len(match) > 1 {
				slog.Debug(
					fmt.Sprintf("Setting env var: '%s'", match[1]),
				)
				fv.SetString(os.Getenv(match[1]))
			}
		}
	}
}
