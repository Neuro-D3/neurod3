# PR Preview Environment Setup Guide

This guide walks you through setting up a PR preview environment on Oracle Cloud Infrastructure (OCI) using a single VM with Docker Compose, Caddy reverse proxy, and GitHub Actions.

## Architecture Overview

The PR preview system runs entirely on a single OCI VM:

- **GitHub Actions self-hosted runner**: Executes workflows on the VM
- **Docker Compose**: Manages isolated PR environments
- **Caddy reverse proxy**: Routes traffic to PR-specific services
- **All services**: Run as containers on the same VM

Each PR gets:
- Isolated Docker network (`pr-<NUMBER>-pr-network`)
- Isolated volumes (prefixed with `pr-<NUMBER>`)
- Unique subdomains:
  - `pr-<NUMBER>.airflow.preview.example.com`
  - `pr-<NUMBER>.app.preview.example.com`
  - `pr-<NUMBER>.api.preview.example.com`
  - `pr-<NUMBER>.pgadmin.preview.example.com`

## Prerequisites

- Oracle Cloud Infrastructure account
- A domain name (or subdomain) for preview environments
- GitHub repository with Actions enabled
- SSH access to OCI VM

## Step 1: Provision OCI VM

### 1.1 Create Compute Instance

1. Log in to Oracle Cloud Console
2. Navigate to **Compute** > **Instances**
3. Click **Create Instance**
4. Configure:
   - **Name**: `pr-preview-vm` (or your preferred name)
   - **Image**: Ubuntu 22.04 LTS or later
   - **Shape**: At least 2 OCPUs, 8GB RAM (adjust based on expected load)
   - **Networking**: Assign a public IP address
   - **SSH Keys**: Add your public SSH key

### 1.2 Configure Security List

1. Navigate to **Networking** > **Virtual Cloud Networks**
2. Select your VCN
3. Go to **Security Lists** > **Default Security List**
4. Add Ingress Rules:
   - **Source**: `0.0.0.0/0`
   - **IP Protocol**: TCP
   - **Destination Port Range**: `22` (SSH)
   - **Destination Port Range**: `80` (HTTP)
   - **Destination Port Range**: `443` (HTTPS)

### 1.3 Connect to VM

```bash
ssh ubuntu@<VM_PUBLIC_IP>
```

## Step 2: Run Setup Script

### 2.1 Clone Repository (Optional)

If you want to use the setup script from the repository:

```bash
git clone <your-repo-url>
cd <repo-name>
```

### 2.2 Run Setup Script

```bash
# Make script executable
chmod +x deploy/oci/setup-vm.sh

# Run as root
sudo ./deploy/oci/setup-vm.sh
```

The script will:
- Update system packages
- Install Docker and Docker Compose v2
- Install Caddy
- Configure firewall
- Set up directories and services
- Create systemd service for Caddy

### 2.3 Verify Installation

```bash
# Check Docker
docker --version
docker compose version

# Check Caddy
caddy version

# Check services
sudo systemctl status docker
sudo systemctl status caddy-pr-preview
```

## Step 3: Configure Caddy

### 3.1 Copy Caddyfile

```bash
# Copy Caddyfile to the configured location
sudo cp deploy/caddy/Caddyfile /opt/pr-preview/caddy/Caddyfile
sudo chown caddy:caddy /opt/pr-preview/caddy/Caddyfile
```

### 3.2 Start Caddy Service

```bash
sudo systemctl start caddy-pr-preview
sudo systemctl enable caddy-pr-preview
sudo systemctl status caddy-pr-preview
```

### 3.3 Verify Caddy is Running

```bash
# Check if Caddy is listening on ports 80, 443, and 2019
sudo netstat -tlnp | grep -E ':(80|443|2019)'
```

## Step 4: Configure DNS

### 4.1 Get VM Public IP

```bash
curl -s ifconfig.me
```

### 4.2 Configure DNS Records

Add wildcard DNS records pointing to your VM's public IP:

**Type**: A Record  
**Name**: `*.preview` (or `*.airflow.preview`, `*.app.preview`, `*.pgadmin.preview`)  
**Value**: `<VM_PUBLIC_IP>`  
**TTL**: 300 (or your preference)

Example for `example.com`:
- `*.preview.example.com` → `<VM_PUBLIC_IP>`

Or create specific subdomains:
- `*.airflow.preview.example.com` → `<VM_PUBLIC_IP>`
- `*.app.preview.example.com` → `<VM_PUBLIC_IP>`
- `*.api.preview.example.com` → `<VM_PUBLIC_IP>`
- `*.pgadmin.preview.example.com` → `<VM_PUBLIC_IP>`

### 4.3 Verify DNS

```bash
# Test DNS resolution
dig pr-123.airflow.preview.example.com
nslookup pr-123.app.preview.example.com
```

## Step 5: Set Up GitHub Actions Runner

### 5.1 Get Runner Token

1. Go to your GitHub repository
2. Navigate to **Settings** > **Actions** > **Runners**
3. Click **New self-hosted runner**
4. Select **Linux** and **x64**
5. Copy the registration token (you'll need it in the next step)

### 5.2 Install Runner on VM

```bash
# Create runner directory
cd /opt/pr-preview
mkdir -p actions-runner && cd actions-runner

# Download runner (check for latest version at https://github.com/actions/runner/releases)
curl -o actions-runner-linux-x64-2.311.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-linux-x64-2.311.0.tar.gz

# Extract
tar xzf ./actions-runner-linux-x64-2.311.0.tar.gz

# Configure (replace TOKEN and REPO)
./config.sh --url https://github.com/OWNER/REPO --token YOUR_TOKEN

# Install as a service
sudo ./svc.sh install

# Start the service
sudo ./svc.sh start

# Check status
sudo ./svc.sh status
```

### 5.3 Verify Runner

1. Go to **Settings** > **Actions** > **Runners**
2. You should see your runner listed as "Online"

## Step 6: Configure GitHub Repository

### 6.1 Set Repository Variables

Go to **Settings** > **Secrets and variables** > **Actions** > **Variables**:

- `DOMAIN`: Your preview domain (e.g., `preview.example.com`)
- `CADDY_API_URL`: Caddy admin API URL (default: `http://localhost:2019`)

### 6.2 Verify Workflow File

Ensure `.github/workflows/pr-preview.yml` is in your repository.

## Step 7: Test the System

### 7.1 Create a Test PR

1. Create a new branch
2. Make a small change
3. Open a pull request
4. Add the `deploy-preview` label

### 7.2 Monitor Deployment

1. Go to **Actions** tab in GitHub
2. Watch the "PR Preview Environment" workflow
3. Check that it:
   - Validates the PR (not a fork, collaborator check)
   - Builds and deploys containers
   - Registers Caddy routes
   - Comments on the PR with preview URLs

### 7.3 Verify Preview URLs

Once deployed, you should be able to access:
- `https://pr-<NUMBER>.airflow.preview.example.com`
- `https://pr-<NUMBER>.app.preview.example.com`
- `https://pr-<NUMBER>.api.preview.example.com`
- `https://pr-<NUMBER>.pgadmin.preview.example.com`

## Usage

### Deploying a Preview

1. Open a pull request
2. Add the `deploy-preview` label
3. Wait for the workflow to complete (~2-5 minutes)
4. Check the PR comment for preview URLs

**Important**: Only one PR can have the `deploy-preview` label at a time. If you add the label to a new PR while another PR already has it, the system will automatically:
- Remove the label from the previous PR
- Tear down the previous PR's environment
- Deploy the new PR's environment
- Comment on both PRs to notify about the change

### Updating a Preview

1. Push new commits to the PR branch
2. The workflow will automatically redeploy if the label is present

### Removing a Preview

- **Option 1**: Remove the `deploy-preview` label
- **Option 2**: Close the pull request

Both actions will trigger teardown and cleanup.

## Troubleshooting

### Caddy Not Starting

```bash
# Check Caddy status
sudo systemctl status caddy-pr-preview

# View Caddy logs
sudo journalctl -u caddy-pr-preview -f

# Check Caddyfile syntax
sudo caddy validate --config /opt/pr-preview/caddy/Caddyfile
```

### Containers Not Starting

```bash
# Check Docker status
sudo systemctl status docker

# View container logs
docker compose -p pr-<NUMBER> logs

# Check if containers are running
docker ps -a | grep pr-<NUMBER>
```

### Routes Not Working

```bash
# Check if routes are registered
curl http://localhost:2019/config/apps/http/servers/srv0/routes | jq

# Manually register routes
cd /opt/pr-preview
./scripts/register-routes.sh <PR_NUMBER> <DOMAIN>

# Check Caddy logs for errors
sudo journalctl -u caddy-pr-preview -f
```

### GitHub Runner Not Picking Up Jobs

```bash
# Check runner status
cd /opt/pr-preview/actions-runner
sudo ./svc.sh status

# View runner logs
tail -f _diag/Runner_*.log

# Restart runner
sudo ./svc.sh stop
sudo ./svc.sh start
```

### DNS Issues

```bash
# Verify DNS resolution
dig pr-123.airflow.preview.example.com

# Check if DNS is pointing to correct IP
nslookup pr-123.app.preview.example.com

# Test from VM
curl -H "Host: pr-123.airflow.preview.example.com" http://localhost
```

### Port Conflicts

```bash
# Check what's using ports 80, 443, 2019
sudo lsof -i :80
sudo lsof -i :443
sudo lsof -i :2019

# Stop conflicting services
sudo systemctl stop apache2  # if Apache is running
sudo systemctl stop nginx     # if Nginx is running
```

### Health Checks Failing

```bash
# Manually run health check
cd /opt/pr-preview
./scripts/health-check.sh <PR_NUMBER>

# Check individual services
docker exec pr-<NUMBER>-airflow-webserver-1 curl http://localhost:8080/health
docker exec pr-<NUMBER>-api-1 curl http://localhost:8000/api/health
```

### Cleanup Stuck Environments

```bash
# List all PR environments
docker ps -a | grep pr-

# Force remove a specific PR environment
docker compose -p pr-<NUMBER> down -v --remove-orphans

# Remove all PR environments (use with caution!)
docker ps -a | grep pr- | awk '{print $1}' | xargs docker rm -f
docker volume ls | grep pr- | awk '{print $2}' | xargs docker volume rm
```

## Security Considerations

### Fork Protection

The workflow automatically rejects PRs from forks. Only PRs from the same repository are deployed.

### Collaborator Check

Only users with write access to the repository can trigger deployments.

### Network Isolation

Each PR environment runs in its own Docker network, preventing cross-PR communication.

### Volume Cleanup

The `-v` flag ensures all volumes are removed when tearing down, preventing data leakage between PRs.

### Rate Limiting

The system enforces that only one PR can have a preview environment active at a time. When a new PR gets the `deploy-preview` label, any existing preview environment is automatically torn down. This helps manage resource usage on the single VM.

If you need multiple concurrent previews, consider:
- Provisioning additional VMs
- Using a more powerful VM instance
- Implementing a queue system for preview deployments

## Monitoring

### Check Resource Usage

```bash
# CPU and memory
htop

# Disk usage
df -h
docker system df

# Network usage
iftop
```

### View Logs

```bash
# Caddy logs
sudo journalctl -u caddy-pr-preview -f

# Docker logs
docker compose -p pr-<NUMBER> logs -f

# GitHub runner logs
tail -f /opt/pr-preview/actions-runner/_diag/Runner_*.log
```

## Maintenance

### Update Caddy Configuration

```bash
# Edit Caddyfile
sudo nano /opt/pr-preview/caddy/Caddyfile

# Reload Caddy
sudo systemctl reload caddy-pr-preview
```

### Update Helper Scripts

```bash
# Copy updated scripts from repository
sudo cp deploy/scripts/*.sh /opt/pr-preview/scripts/
sudo chmod +x /opt/pr-preview/scripts/*.sh
```

### Update GitHub Runner

```bash
cd /opt/pr-preview/actions-runner
sudo ./svc.sh stop
# Download new version
curl -o actions-runner-linux-x64-<VERSION>.tar.gz -L https://github.com/actions/runner/releases/download/v<VERSION>/actions-runner-linux-x64-<VERSION>.tar.gz
tar xzf ./actions-runner-linux-x64-<VERSION>.tar.gz
sudo ./svc.sh install
sudo ./svc.sh start
```

## Cost Optimization

- Use OCI Always Free tier if available
- Set up automatic cleanup for old PR environments
- Monitor resource usage and scale VM size as needed
- Consider using spot instances for cost savings

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review GitHub Actions workflow logs
3. Check Caddy and Docker logs
4. Verify DNS and network configuration

