package config

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

var envRegex = regexp.MustCompile(`\${(\w+)}`)

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

func getEnv[T float64 | string | int | bool | time.Duration](key string, defaultVal T, required bool) T {
	val, ok := os.LookupEnv(key)
	if !ok {
		if !required {
			return defaultVal
		} else {
			log.Fatalf("missing required environment variable %s", key)
		}
	}

	var out T
	switch ptr := any(&out).(type) {
	case *string:
		{
			*ptr = val
		}
	case *int:
		{
			v, err := strconv.Atoi(val)
			if err != nil {
				return defaultVal
			}
			*ptr = v
		}
	case *bool:
		{
			v, err := strconv.ParseBool(val)
			if err != nil {
				return defaultVal
			}
			*ptr = v
		}
	case *time.Duration:
		{
			v, err := time.ParseDuration(val)
			if err != nil {
				return defaultVal
			}
			*ptr = v
		}
	case *float64:
		{
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return defaultVal
			}
			*ptr = v
		}
	default:
		{
			log.Fatalf("unsupported type %T", out)
		}
	}

	return out
}
