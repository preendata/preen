---
description: how to configure hypha models.
---

# Models

Hypha models are defined as a YAML file. The model file is used to define the data sources, the query to be executed, and the type of query to be executed.

## Model Configuration Options

| Option | Description | Required | Applicable Types |
|--------|-------------|----------|------------------|
| `name` | The unique name of the model | Yes | All |
| `type` | The type of the model (e.g., `sql`, `mongodb`) | Yes | All |
| `format` | The format of the data | Only for `file` type | `file` |
| `query` | The query to be executed | Yes for `database` type | `database` |
| `options` | Additional options for the model (e.g., file format, delimiter, header) | No | All (specific options vary by type) |
| `file_patterns` | The file patterns to be used for matching files | Only for `file` type | `file` |

## Code References

- [models.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/models.go)
