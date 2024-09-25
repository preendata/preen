---
description: how to configure hypha sources.
---

# Sources

Hypha sources are defined as a YAML file. The source file is used to define the data sources, the query to be executed, and the type of query to be executed.

## Source Configuration Options

| Option          | Description                                                             | Required                | Applicable Types                    |
| --------------- | ----------------------------------------------------------------------- | ----------------------- | ----------------------------------- |
| `name`          | The unique name of the source                                            | Yes                     | All                                 |
| `engine`        | The type of the source (e.g.`database`, `file`)                          | Yes                     | All                                 |
| `connection`    | The connection details for the source (e.g. database connection details) | Yes                     | All                                 |
| `models`        | The models to be used for the source                                     | Yes                     | All                                 |

## Source Connection Details

| Option        | Description                                |
|---------------|--------------------------------------------|
| `host`        | The host of the source                     |
| `port`        | The port of the source                     |
| `database`    | The database of the source                 |
| `username`    | The username of the source                 |
| `password`    | The password of the source                 |
| `auth_source` | The authentication source for MongoDB      |
| `bucket_name` | The bucket name for AWS S3 models          |
| `region`      | The AWS region for S3 models               |

## Code References

- [sources.go](../../../internal/engine/sources.go)
