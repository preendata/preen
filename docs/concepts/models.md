---
description: what is a model?
---

# Models

## Overview

A Preen **Model** is a fundamental concept that defines how data is accessed and structured for local querying. It acts as a bridge between your raw data sources and the Preen system, allowing for targeted data retrieval.

## Definition

A Model is a storage system-dependent configuration that specifies:

1. The source of the data
2. The structure or schema of the data
3. Any filtering or transformation to be applied

Models narrow down the set of data to be used for local querying, ensuring that only relevant information is processed.

## Examples

Models can be configured for various types of storage systems. Here are some examples:

### SQL Databases

These models are defined as a YAML file that contains a SQL query.

```yaml
# FILENAME: ~/.preen/models/users.yaml
name: users # This name needs to be unique
type: database
query: |
  select
    users.id,
    users.first_name,
    users.last_name,
    users.birthday
  from
    users;
```

### File Systems

These models are configured as a YAML file and contain configurations specific to the underlying file storage system. Here is an example of a model using Amazon S3 and a csv file. The full list of options can be found here.

```yaml
# FILENAME: ~/.preen/models/users.yaml
name: users # This name needs to be unique
type: file
file_patterns:
  - "users/v1/**.csv" # This will match all csv files under the users/v1 prefix
format: csv
options:
  auto_detect: true
  header: true
  delim: ","
  quote: "\""
  escape: "\""
  new_line: "\\r\\n"
  filename: true
  union_by_name: true
```

## Benefits of Using Models

1. **Data Isolation**: Models allow you to work with specific subsets of your data, improving performance and reducing noise.
2. **Abstraction**: They provide a layer of abstraction between your raw data sources and your Preen queries.
3. **Flexibility**: Models can be easily adjusted to accommodate changes in data structure or source without affecting the rest of your Preen setup.
4. **Reusability**: Once defined, Models can be shared and reused by different users and teams within your organization.

## CLI Commands

```bash
preen model build # Builds all models
preen model build --target users # Target a specific model
```

For detailed configuration reference see [models.md](../documentation/config/models.md "mention")
