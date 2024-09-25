---
description: How to create a model to query a source.
---

# Creating Models

[models.md](../../concepts/models.md "mention")are how you define the data you want to work with from a given data source. Don't think of a Model as your final result or query set, rather its all the relevant data from which you may query your final result set.&#x20;

Read more about the rationale behind [models.md](../../concepts/models.md "mention")on its concept page.

#### Defining a Model

You can define models in two ways, adding a `models.yaml` file to the `HYPHA_CONFIG_PATH` or adding individual model files to the  `~/.hypha/models` directory. You may save a model file anywhere you'd like, so long as its parent directory is specified by `HYPHA_MODELS_PATH`

Here's an example `sql` model. **Note that column names need to be fully qualified, i.e.  users.id instead of id.**

```yaml
# FILENAME: ~/.hypha/models/users.yaml
name: users # This name needs to be unique
type: sql
query: |
  select
    users.id,
    users.first_name,
    users.last_name,
    users.birthday
  from
    users;
```

#### Registering a Model with a Source

Consider a simplified [source.md](../../concepts/source.md "mention") config from the last page, pared down to one data source. You register the users model with the source as follows.

```yaml
# FILENAME: ~/.hypha/sources.yaml
sources:
  - name: postgres-model
    engine: postgres
    connection:
      host: localhost
      port: 33061
      database: postgres
      username: root
      password: myp@assword
    models:
      - users
```

You've now completed the setup and can validate the correctness of your work by running `hypha source validate`.

There's much more to know about models. To learn more, visit the [Broken link](broken-reference "mention") or go directly to the [Broken link](broken-reference "mention")page.
