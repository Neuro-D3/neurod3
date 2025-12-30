#!/bin/bash
# User data script for OCI instance
# This script runs on first boot

set -e

PROJECT_NAME="${project_name}"

# Update system
apt-get update
apt-get upgrade -y

# Install basic packages
apt-get install -y \
    curl \
    wget \
    git \
    jq \
    unzip \
    htop \
    ufw \
    ca-certificates \
    gnupg \
    lsb-release

# Create project directory
mkdir -p /opt/${project_name}
mkdir -p /opt/${project_name}/logs

# Set up log file
exec > >(tee -a /var/log/user-data.log)
exec 2>&1

echo "User data script completed at $(date)" >> /var/log/user-data.log

# Note: Docker, Caddy, and GitHub runner will be installed by setup-vm.sh
# This script just prepares the base system

