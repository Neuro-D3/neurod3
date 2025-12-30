# Quick Start: Running Terraform from Oracle Cloud Shell

Oracle Cloud Shell is the easiest way to run Terraform - it's pre-authenticated and has everything you need!

## Step 1: Open Cloud Shell

1. Log into Oracle Cloud Console
2. Click the Cloud Shell icon (top right) or go to **Developer Tools** > **Cloud Shell**
3. Wait for Cloud Shell to initialize

## Step 2: Clone Your Repository

```bash
git clone <your-repo-url>
cd <repo-name>/deploy/infra
```

## Step 3: Generate SSH Key (if needed)

Cloud Shell can generate an SSH key for you:

```bash
# Generate a new SSH key (RSA for FIPS compliance, no passphrase needed for automation)
ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/oci_key

# Get the public key
cat ~/.ssh/oci_key.pub
```

## Step 4: Get Your Tenancy OCID (One-Time)

**Easiest way - From OCI Console:**
1. Click the menu (top left) > **Administration** > **Tenancy Details**
2. Copy the **OCID** (it looks like: `ocid1.tenancy.oc1..aaaaaaa...`)

**Or from Cloud Shell (if config file exists):**
```bash
grep "^tenancy=" ~/.oci/config 2>/dev/null | cut -d'=' -f2
```

**Or from any compartment:**
```bash
oci iam compartment list --all --compartment-id-in-subtree true --limit 1 --query 'data[0].compartment-id' --raw-output
```

## Step 5: Configure Terraform

```bash
# Copy the example file
cp terraform.tfvars.example terraform.tfvars

# Edit it (Cloud Shell has nano/vim built-in)
nano terraform.tfvars
```

**Minimum configuration** (just 3 things!):
```hcl
region = "us-sanjose-1"  # Your region

tenancy_ocid = "ocid1.tenancy.oc1..aaaaaaa..."  # From step 4

ssh_public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQC..."  # From step 3
```

That's it! Cloud Shell automatically handles:
- ✅ Authentication (already logged in)
- ✅ Tenancy OCID (auto-detected)
- ✅ OCI CLI config (pre-configured)

## Step 6: Run Terraform

```bash
# Initialize Terraform
terraform init

# See what will be created
terraform plan

# Create everything
terraform apply
```

Type `yes` when prompted.

## Step 7: Get Your Instance Details

```bash
# Get the public IP
terraform output instance_public_ip

# Get SSH command
terraform output ssh_connection_command
```

## Step 8: SSH to Your Instance

```bash
# Use the private key you generated
ssh -i ~/.ssh/oci_key ubuntu@<instance-public-ip>
```

## That's It!

Your VM is ready. Now follow the setup instructions from the main README to install Docker, Caddy, and GitHub Actions runner.

## Tips

- **Cloud Shell persistence**: Your home directory persists between sessions
- **File upload**: Use the Cloud Shell menu to upload files if needed
- **Download files**: Right-click in Cloud Shell to download files
- **Session timeout**: Cloud Shell sessions timeout after 20 minutes of inactivity

## Troubleshooting

**"Authentication failed"**
- Cloud Shell should be auto-authenticated. Try refreshing the Cloud Shell.

**"Region not found"**
- Check you're using the correct region code (e.g., `us-sanjose-1`, `us-phoenix-1`)

**"SSH connection refused"**
- Wait 2-3 minutes after `terraform apply` for the instance to fully boot
- Check security list allows SSH (port 22) - should be automatic

