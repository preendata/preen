---
description: Define the data sources your Preen session can connect to
---

# Configuring Sources

Preen maintains a configuration file in `$HOME/.preen/sources.yml` by default. This is can be overridden via the `PREEN_CONFIG_PATH` environment variable.

A config file might look like this:

```yaml
sources:
  - name: s3-model
    engine: s3
    connection:
      username: ACCJBKSHJLFALJAFJB
      password: BALJBS786asdhjaa87ads6asdas23232
      host: us-east-1
      database: preendb-internal-transfer
      AuthSource: mock-user-data-1.csv
  - name: postgres-model
    engine: postgres
    connection:
      host: localhost
      port: 33061
      database: postgres
      username: root
      password: myp@assword
  - name: mongo-model
    engine: mongodb
    connection:
      host: missing
      port: this
      database: stuff
```

In a nutshell, your configuration is primarily a list of data sources, credentials, and their engine classification (see [config](../../documentation/config/ "mention")for list of supported engines). **Be sure to add this file to your `.gitignore` if you are keeping it somewhere version controlled.**
