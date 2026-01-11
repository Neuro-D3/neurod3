#!/bin/bash
# OCI VM Setup Script for PR Preview Environment
# This script sets up an Ubuntu VM with Docker, Caddy, and GitHub Actions runner
# Usage: sudo ./setup-vm.sh

set -e

echo "=========================================="
echo "OCI VM Setup for PR Preview Environment"
echo "=========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Update system
echo "Step 1: Updating system packages..."
apt-get update
apt-get upgrade -y

# Install prerequisites
echo ""
echo "Step 2: Installing prerequisites..."
apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    jq \
    git \
    unzip \
    ufw

# Install Docker
echo ""
echo "Step 3: Installing Docker..."
if ! command -v docker &> /dev/null; then
    # Add Docker's official GPG key
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg

    # Set up repository
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

    # Install Docker Engine
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

    # Start and enable Docker
    systemctl start docker
    systemctl enable docker

    echo "✓ Docker installed"
else
    echo "✓ Docker already installed"
fi

# Verify Docker Compose v2
echo ""
echo "Step 4: Verifying Docker Compose v2..."
if docker compose version &> /dev/null; then
    echo "✓ Docker Compose v2 is available"
    docker compose version
else
    echo "✗ Docker Compose v2 not found"
    exit 1
fi

# Install Caddy
echo ""
echo "Step 5: Installing Caddy..."
if ! command -v caddy &> /dev/null; then
    apt-get install -y debian-keyring debian-archive-keyring apt-transport-https
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list
    apt-get update
    apt-get install -y caddy

    echo "✓ Caddy installed"
else
    echo "✓ Caddy already installed"
fi

# Configure firewall
echo ""
echo "Step 6: Configuring firewall..."
ufw --force enable
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
# NOTE: Do NOT open 2019 publicly. The Caddy admin API should only be reachable locally.
echo "✓ Firewall configured"

# Create directories
echo ""
echo "Step 7: Creating directories..."
mkdir -p /opt/pr-preview
mkdir -p /opt/pr-preview/caddy
mkdir -p /opt/pr-preview/scripts
mkdir -p /opt/pr-preview/logs
mkdir -p /var/log/pr-preview
echo "✓ Directories created"

# Set up Caddy service
echo ""
echo "Step 8: Setting up Caddy service..."
cat > /etc/systemd/system/caddy-pr-preview.service <<'EOF'
[Unit]
Description=Caddy Reverse Proxy for PR Previews
Documentation=https://caddyserver.com/docs/
After=network.target

[Service]
Type=notify
User=caddy
Group=caddy
ExecStart=/usr/bin/caddy run --environ --config /opt/pr-preview/caddy/Caddyfile
ExecReload=/usr/bin/caddy reload --config /opt/pr-preview/caddy/Caddyfile --force
TimeoutStopSec=5s
LimitNOFILE=1048576
LimitNPROC=512
PrivateTmp=true
ProtectSystem=full
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
EOF

# Create caddy user if it doesn't exist
if ! id "caddy" &>/dev/null; then
    useradd -r -s /usr/sbin/nologin caddy
fi

# Set permissions
chown -R caddy:caddy /opt/pr-preview/caddy
chown -R caddy:caddy /var/log/pr-preview

# Create Caddyfile if it doesn't exist
if [ ! -f /opt/pr-preview/caddy/Caddyfile ]; then
    cat > /opt/pr-preview/caddy/Caddyfile <<'EOF'
{
	admin localhost:2019
	auto_https off
}

:80 {
	respond "No PR preview environment found" 404
}
EOF
    chown caddy:caddy /opt/pr-preview/caddy/Caddyfile
fi

systemctl daemon-reload
systemctl enable caddy-pr-preview.service
echo "✓ Caddy service configured"

# GitHub Actions Runner setup
echo ""
echo "Step 9: GitHub Actions Runner setup..."
echo "You will need to set up the GitHub Actions runner manually:"
echo "1. Go to your repository Settings > Actions > Runners"
echo "2. Click 'New self-hosted runner'"
echo "3. Follow the instructions to download and configure the runner"
echo ""
echo "Or run the following commands (replace TOKEN and REPO):"
echo "  cd /opt/pr-preview"
echo "  mkdir -p actions-runner && cd actions-runner"
echo "  curl -o actions-runner-linux-x64-2.311.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-linux-x64-2.311.0.tar.gz"
echo "  tar xzf ./actions-runner-linux-x64-2.311.0.tar.gz"
echo "  ./config.sh --url https://github.com/OWNER/REPO --token TOKEN"
echo "  sudo ./svc.sh install"
echo "  sudo ./svc.sh start"

# Create runner directory
mkdir -p /opt/pr-preview/actions-runner
RUN_USER="${SUDO_USER:-root}"
chown -R "$RUN_USER":"$RUN_USER" /opt/pr-preview/actions-runner

# Set up log rotation
echo ""
echo "Step 10: Setting up log rotation..."
cat > /etc/logrotate.d/pr-preview <<'EOF'
/var/log/pr-preview/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0644 root root
}
EOF
echo "✓ Log rotation configured"

# Create helper script directory
echo ""
echo "Step 11: Setting up helper scripts..."
cat > /opt/pr-preview/scripts/register-routes.sh <<'SCRIPT_EOF'
#!/bin/bash
# This script will be replaced by the one from the repository
# Placeholder for route registration
SCRIPT_EOF

cat > /opt/pr-preview/scripts/remove-routes.sh <<'SCRIPT_EOF'
#!/bin/bash
# This script will be replaced by the one from the repository
# Placeholder for route removal
SCRIPT_EOF

cat > /opt/pr-preview/scripts/health-check.sh <<'SCRIPT_EOF'
#!/bin/bash
# This script will be replaced by the one from the repository
# Placeholder for health checks
SCRIPT_EOF

chmod +x /opt/pr-preview/scripts/*.sh
chown -R $SUDO_USER:$SUDO_USER /opt/pr-preview/scripts
echo "✓ Helper scripts directory created"

# Set up environment variables file
echo ""
echo "Step 12: Creating environment configuration..."
if [ ! -f /opt/pr-preview/.env ]; then
    cat > /opt/pr-preview/.env <<'EOF'
# PR Preview Environment Configuration
# Update these values as needed

# Domain for preview environments (e.g., preview.example.com)
DOMAIN=preview.example.com

# Caddy admin API URL
CADDY_API_URL=http://localhost:2019

# GitHub repository (for runner configuration)
GITHUB_REPO=OWNER/REPO
EOF
    chown $SUDO_USER:$SUDO_USER /opt/pr-preview/.env
    echo "✓ Environment file created at /opt/pr-preview/.env"
    echo "  Please update the values in this file"
else
    echo "✓ Environment file already exists"
fi

# Summary
echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Update /opt/pr-preview/.env with your domain and repository"
echo "2. Copy your Caddyfile to /opt/pr-preview/caddy/Caddyfile"
echo "3. Copy helper scripts from deploy/scripts/ to /opt/pr-preview/scripts/"
echo "4. Set up GitHub Actions runner (see instructions above)"
echo "5. Start Caddy service: sudo systemctl start caddy-pr-preview"
echo "6. Configure DNS: *.preview.example.com -> $(curl -s ifconfig.me)"
echo ""
echo "To start Caddy:"
echo "  sudo systemctl start caddy-pr-preview"
echo ""
echo "To check Caddy status:"
echo "  sudo systemctl status caddy-pr-preview"
echo ""
echo "To view Caddy logs:"
echo "  sudo journalctl -u caddy-pr-preview -f"
echo ""




