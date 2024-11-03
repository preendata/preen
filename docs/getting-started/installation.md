---
description: how to install preen.
---

# Installation

You can install Preen a few different ways. Note that the binary installation is the easiest method if you want to get started quickly. We support building from source if you want to have a local copy of the application code and make changes.

## Homebrew

Download the executable via our Homebrew cask.

```
brew tap preendata/preen
brew install preen
```

## Download binary

You can download a binary for your operating system and architecture from the [GitHub Releases](https://github.com/preendata/preen/releases) page.

```bash
# Using curl
sh -c "$(curl -fsSL https://raw.githubusercontent.com/preendata/preen/main/build/install.sh)"
```

```bash
# Using wget
sh -c "$(wget https://raw.githubusercontent.com/preendata/preen/main/build/install.sh -O -)"
```

## Build from source

To build Preen from source, you need to have Go 1.23.0 or later installed on your system. Then, you can build the application using the following commands:

```bash
git clone https://github.com/preendata/preen.git
cd preen
make build
```

This will create a `preen` binary in the `bin` directory. You can add this to your `PATH` if you want to use the `preen` command from anywhere.

### Validation

Test that you've correctly installed the application by executing

```bash
preen -h
```
