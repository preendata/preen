#!/bin/bash

set -e

# Define variables
VERSION="v0.0.1"
GITHUB_REPO="hyphasql/hypha"
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

# Construct download URL
DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${VERSION}/hypha-${OS}_${ARCH}-${VERSION}.tar.gz"

# Download and install
echo "Downloading Hypha ${VERSION} for ${OS}_${ARCH}..."
if ! curl -L -o hypha.tar.gz "$DOWNLOAD_URL"; then
    echo "Failed to download Hypha. Please check your internet connection and try again."
    exit 1
fi

echo "Extracting..."
if ! tar -xvzf hypha.tar.gz; then
    echo "Failed to extract the archive."
    rm hypha.tar.gz
    exit 1
fi

echo "Installing to $INSTALL_DIR..."
if [ -w "$INSTALL_DIR" ]; then
    mv hypha "$INSTALL_DIR"
else
    sudo mv hypha "$INSTALL_DIR"
fi

echo "Cleaning up..."
rm hypha.tar.gz

echo "Hypha ${VERSION} has been successfully installed to $INSTALL_DIR"