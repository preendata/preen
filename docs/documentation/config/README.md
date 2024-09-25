---
description: how to configure hypha.
---

# Config

Hypha is configured using a YAML file. The config file is used to define the sources, models, and other configurations. You can customize the location of the config file by setting the `HYPHA_CONFIG_PATH` environment variable. If no environment variable is set, Hypha will look for a file called `~/.hypha/sources.yaml`. You can also configure a custom path where Hypha will look for model files by setting the `HYPHA_MODELS_PATH` environment variable. If no environment variable is set, Hypha will look for models configured in `~/.hypha/models.yaml`.

## Config File Reference

- [Sources](sources.md)
- [Models](models.md)

## Code References

- [env.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/env.go)
- [config.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/config.go)
- [sources.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/sources.go)
- [models.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/models.go)
