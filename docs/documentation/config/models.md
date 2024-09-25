---
description: how to configure hypha models.
---

# Models

Hypha models are defined as a YAML file. The model file is used to define the data sources, the query to be executed, and the type of query to be executed.

## Model Configuration Options

| Option          | Description                                                             | Required                | Applicable Types                    |
| --------------- | ----------------------------------------------------------------------- | ----------------------- | ----------------------------------- |
| `name`          | The unique name of the model                                            | Yes                     | All                                 |
| `type`          | The type of the model (e.g.`database`, `file`)                          | Yes                     | All                                 |
| `format`        | The format of the data (e.g. csv)                                       | Only for `file` type    | `file`                              |
| `query`         | The query to be executed                                                | Yes for `database` type | `database`                          |
| `options`       | Additional options for the model (e.g., file format, delimiter, header) | No                      | All (specific options vary by type) |
| `file_patterns` | The file patterns to be used for matching files                         | Only for `file` type    | `file`                              |

## Code References

* [models.go](../../../internal/engine/models.go)
