package engine

import (
	"fmt"
	"path/filepath"
	"reflect"
)

func ingestS3Model(r *Retriever) error {
	switch r.Format {
	case "csv":
		optionsString, err := getCSVOptions(r.Options)
		if err != nil {
			return fmt.Errorf("failed to get csv options: %v", err)
		}
		query := fmt.Sprintf(
			`create or replace table main.%s as select * from read_csv(%s,%s)
			`, r.TableName, formatFilePatterns(r), *optionsString,
		)
		Debug(fmt.Sprintf("running query: %s", query))
		if err := ddbExec(query); err != nil {
			Debug(fmt.Sprintf("running query: %s", query))
			return fmt.Errorf("failed to create file model table %s: %v", r.ModelName, err)
		}
	default:
		return fmt.Errorf("unsupported model file format %s", r.Format)
	}
	return nil
}

func formatFilePatterns(r *Retriever) string {
	queryString := "["
	for i, v := range *r.FilePatterns {
		if i == len(*r.FilePatterns)-1 {
			queryString += fmt.Sprintf("'%s://%s'", r.Source.Engine, filepath.Join(r.Source.Connection.BucketName, v)) + "]"
			break
		}
		queryString += fmt.Sprintf("'%s://%s', ", r.Source.Engine, filepath.Join(r.Source.Connection.BucketName, v))
	}
	return queryString
}

func getCSVOptions(o Options) (*string, error) {
	options := reflect.VisibleFields(reflect.TypeOf(o))
	queryString := new(string)
	for _, option := range options {
		if _, ok := option.Tag.Lookup("default"); !ok {
			return nil, fmt.Errorf("missing default value for option %s", option.Name)
		}
		if _, ok := option.Tag.Lookup("yaml"); !ok {
			return nil, fmt.Errorf("missing yaml tag for option %s", option.Name)
		}

		defaultVal := option.Tag.Get("default")
		optionName := option.Tag.Get("yaml")
		optionValue := getDefaultValue(reflect.ValueOf(o).FieldByName(option.Name).Interface(), defaultVal)
		if optionValue == "" {
			continue
		}
		optionString := fmt.Sprintf("%s = %v", optionName, optionValue)
		if *queryString == "" {
			*queryString += optionString
		} else {
			*queryString = fmt.Sprintf("%s, %s", *queryString, optionString)
		}
	}
	return queryString, nil
}

func getDefaultValue(key any, defaultVal any) any {
	switch key := key.(type) {
	case *[]string:
		// If the key is not set and there is not default value, return an empty string
		if key == nil && defaultVal == "-" {
			return ""
		}
		// If the key is not set and there is a default value, return the default value
		if key == nil && defaultVal != "-" {
			return defaultVal
		}
		// Convert the []string from YAML to a string array for the query
		queryString := "["
		for i, v := range *key {
			if i == len(*key)-1 {
				queryString += v + "]"
				break
			}
			queryString += v + ", "
		}
		return queryString
	case *bool:
		if key == nil {
			return defaultVal
		}
		return *key
	case *string:
		// If the key is not set and there is not default value, return an empty string
		if key == nil && defaultVal == "-" {
			return ""
		}
		// If the key is not set and there is a default value, return the default value
		if key == nil && defaultVal != "-" {
			return fmt.Sprintf("'%s'", defaultVal)
		}
		return fmt.Sprintf("'%s'", *key)
	case *int64:
		if key == nil {
			return defaultVal
		}
		return *key
	case *[]Type:
		// If the key is not set and there is not default value, return an empty string
		if key == nil && defaultVal == "-" {
			return ""
		}
		// If the key is not set and there is a default value, return the default value
		if key == nil && defaultVal != "-" {
			return defaultVal
		}
		// Convert the []Type from YAML to a string object for the query
		queryString := "{"
		for i, v := range *key {
			if i == len(*key)-1 {
				queryString += fmt.Sprintf("'%s': '%s'", v.Name, v.Type) + "}"
				break
			}
			queryString += fmt.Sprintf("'%s': '%s',", v.Name, v.Type)
		}
		return queryString
	}
	return key
}
