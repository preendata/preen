---
description: what is a source?
---

# Sources

## Overview

A Preen **Source** is any data storage system that is listed under the [integrations](../documentation/integrations/ "mention") section, such as a relational database (e.g. Postgres, MySQL etc.), NoSQL database (MongoDB) or file store (Amazon S3).

## Definition

A Source is a storage system-dependent configuration that specifies:

1. The name of the source
2. The type of the source
3. The connection details for the source

## Examples

### Databases

```yaml
sources:
  - name: users-db-us-east-1
    engine: mysql
    connection:
      host: localhost
      port: 5432
      database: mydatabase
      user: ${DB_USER}
      password: ${DB_PASSWORD}
    models:
      - users
```

### Amazon S3

```yaml
sources:
  - name: users-s3-us-east-1
    engine: s3
    connection:
      bucket_name: users-bucket
      region: us-east-1
    models:
      - users
```

For detailed configuration reference see [sources.md](../documentation/config/sources.md "mention")