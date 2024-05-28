package config

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
)

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
