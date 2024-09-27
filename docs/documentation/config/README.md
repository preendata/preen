---
description: how to configure preen.
---

# Config

Preen is configured using a YAML file. The config file is used to define the sources, models, and other configurations. You can customize the location of the config file by setting the `PREEN_CONFIG_PATH` environment variable. If no environment variable is set, Preen will look for a file called `~/.preen/sources.yaml`. You can also configure a custom path where Preen will look for model files by setting the `PREEN_MODELS_PATH` environment variable. If no environment variable is set, Preen will look for models configured in `~/.preen/models.yaml`.

## Config File Reference

- [Sources](sources.md)
- [Models](models.md)

## Code References

- [env.go](https://github.com/preendata/preen/blob/main/internal/engine/env.go)
- [config.go](https://github.com/preendata/preen/blob/main/internal/engine/config.go)
- [sources.go](https://github.com/preendata/preen/blob/main/internal/engine/sources.go)
- [models.go](https://github.com/preendata/preen/blob/main/internal/engine/models.go)
