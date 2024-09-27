#!/bin/bash

set -e

# Define variables
GITHUB_REPO="preendata/preen"
INSTALL_DIR="/usr/local/bin"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

if [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    ARCH="arm64"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

# Fetch the latest release version
echo "Fetching the latest release version..."
VERSION=$(curl -s https://api.github.com/repos/${GITHUB_REPO}/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "$VERSION" ]; then
    echo "Failed to fetch the latest version. Please check your internet connection and try again."
    exit 1
fi

echo "Latest version: $VERSION"

# Construct download URL
DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${VERSION}/preen-${OS}_${ARCH}-${VERSION}.tar.gz"

# Download and install
echo "Downloading Preen ${VERSION} for ${OS}_${ARCH}..."
if ! curl -L -o preen.tar.gz "$DOWNLOAD_URL"; then
    echo "Failed to download Preen. Please check your internet connection and try again."
    exit 1
fi

echo "Extracting..."
if ! tar -xvzf preen.tar.gz; then
    echo "Failed to extract the archive."
    rm preen.tar.gz
    exit 1
fi

echo "Installing to $INSTALL_DIR..."
if [ -w "$INSTALL_DIR" ]; then
    mv preen "$INSTALL_DIR"
else
    sudo mv preen "$INSTALL_DIR"
fi

echo "Cleaning up..."
rm preen.tar.gz

echo "Preen ${VERSION} has been successfully installed to $INSTALL_DIR"