# Installation

#### Download binary

You can download a binary for your operating system and architecture from the [GitHub Releases](https://github.com/hyphasql/hypha/releases) page.

```bash
# Using curl
sh -c "$(curl -fsSL https://raw.githubusercontent.com/hyphasql/hypha/main/build/install.sh)"

# Using wget
sh -c "$(wget https://raw.githubusercontent.com/hyphasql/hypha/main/build/install.sh -O -)"
```

#### Build from source

To build Hypha from source, you need to have Go 1.23.0 or later installed on your system. Then, you can build the application using the following commands:

```bash
git clone https://github.com/hyphasql/hypha.git
cd hypha
make build
```

This will create a `hypha` binary in the `bin` directory. You can add this to your `PATH` if you want to use the `hypha` command from anywhere.

### Validation

Test that you've correctly installed the application by executing

```
hypha -h
```
