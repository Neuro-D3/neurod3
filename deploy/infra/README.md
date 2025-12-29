# Terraform Infrastructure for PR Preview Environment

This directory contains Terraform configurations to provision and manage the Oracle Cloud Infrastructure (OCI) resources for the PR preview environment.

## Resources Created

- **Compute Instance**: Ubuntu 22.04 VM for running Docker, Caddy, and GitHub Actions runner
- **Security List**: Firewall rules for SSH (22), HTTP (80), HTTPS (443), and Caddy admin API (2019)
- **Block Storage** (optional): Additional storage volume if needed

## Prerequisites

1. **OCI Account**: Oracle Cloud Infrastructure account with appropriate permissions
2. **OCI CLI Setup**: API keys configured
3. **Terraform**: Version >= 1.0 installed
4. **OCI Provider**: Terraform OCI provider will be downloaded automatically

## Setup

### 1. Configure OCI API Keys

1. Generate an API key pair:
   ```bash
   openssl genrsa -out ~/.oci/oci_api_key.pem 2048
   openssl rsa -pubout -in ~/.oci/oci_api_key.pem -out ~/.oci/oci_api_key_public.pem
   ```

2. Upload the public key to OCI:
   - Go to **Identity** > **Users** > **Your User** > **API Keys**
   - Click **Add API Key**
   - Upload `oci_api_key_public.pem`

3. Note the fingerprint shown after upload

### 2. Get OCIDs

You'll need the following OCIDs:

- **Tenancy OCID**: Found in **Administration** > **Tenancy Details**
- **User OCID**: Found in **Identity** > **Users** > **Your User**
- **Compartment OCID**: Found in **Identity** > **Compartments** > **Your Compartment**
- **VCN OCID**: Found in **Networking** > **Virtual Cloud Networks**
- **Subnet OCID**: Found in your VCN > **Subnets**

### 3. Configure Terraform Variables

1. Copy the example variables file:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

2. Edit `terraform.tfvars` and fill in your values:
   - OCI authentication details
   - Compartment and networking OCIDs
   - Instance configuration
   - SSH public key

### 4. Initialize Terraform

```bash
cd deploy/infra
terraform init
```

### 5. Plan and Apply

```bash
# Review what will be created
terraform plan

# Apply the configuration
terraform apply
```

### 6. Get Outputs

After applying, get the instance details:

```bash
terraform output
```

The output will include:
- Instance public IP
- SSH connection command
- Setup instructions

## SSH Key Setup

Generate an SSH key pair if you don't have one:

```bash
ssh-keygen -t rsa -b 4096 -C "your-email@example.com" -f ~/.ssh/oci_pr_preview
```

Add the **public key** content to `terraform.tfvars` in the `ssh_public_key` variable.

## Post-Deployment Setup

After Terraform creates the instance:

1. **SSH into the instance**:
   ```bash
   ssh ubuntu@<instance-public-ip>
   ```

2. **Run the setup script**:
   ```bash
   # Clone your repository or copy files
   git clone <your-repo-url>
   cd <repo-name>
   sudo ./deploy/oci/setup-vm.sh
   ```

3. **Configure DNS**:
   - Point `*.preview.<your-domain>` to the instance public IP
   - Or create specific subdomains as needed

4. **Set up GitHub Actions runner**:
   - Follow instructions in `docs/PR_PREVIEW_SETUP.md`

## Managing Resources

### View Resources

```bash
terraform show
```

### Update Resources

Modify `variables.tf` or `terraform.tfvars`, then:

```bash
terraform plan
terraform apply
```

### Destroy Resources

⚠️ **Warning**: This will delete all resources!

```bash
terraform destroy
```

## Variables Reference

| Variable | Description | Default |
|---------|-------------|---------|
| `tenancy_ocid` | OCID of the tenancy | Required |
| `user_ocid` | OCID of the user | Required |
| `fingerprint` | API key fingerprint | Required |
| `private_key_path` | Path to private key | Required |
| `region` | OCI region | `us-ashburn-1` |
| `compartment_ocid` | Compartment OCID | Required |
| `vcn_id` | VCN OCID | Required |
| `subnet_id` | Subnet OCID | Required |
| `vcn_cidr` | VCN CIDR block | `10.0.0.0/16` |
| `project_name` | Project name | `neurod3` |
| `instance_shape` | Instance shape | `VM.Standard.E4.Flex` |
| `instance_ocpus` | Number of OCPUs | `2` |
| `instance_memory_gb` | Memory in GB | `8` |
| `ssh_public_key` | SSH public key | Required |
| `create_additional_storage` | Create block storage | `false` |
| `additional_storage_size_gb` | Storage size in GB | `100` |
| `storage_vpus_per_gb` | Storage performance | `10` |

## Instance Shapes

Common shapes for PR preview environments:

- **VM.Standard.E4.Flex**: Flexible shape (recommended)
  - 1-64 OCPUs
  - 1-1024 GB memory
  - Good for variable workloads

- **VM.Standard2.1**: Fixed shape
  - 1 OCPU
  - 15 GB memory
  - Lower cost option

- **VM.Standard.E3.Flex**: Flexible shape
  - 1-64 OCPUs
  - 1-1024 GB memory
  - Alternative to E4

## Security Considerations

1. **Private Key**: Never commit `terraform.tfvars` or private keys to version control
2. **Security List**: The security list allows SSH from anywhere. Consider restricting to your IP
3. **Caddy Admin API**: Port 2019 is restricted to VCN CIDR. Consider further restrictions
4. **SSH Keys**: Use strong SSH keys and consider key rotation

## Troubleshooting

### Authentication Errors

- Verify API key fingerprint matches
- Check private key path is correct
- Ensure private key has correct permissions: `chmod 600 ~/.oci/oci_api_key.pem`

### Resource Not Found

- Verify all OCIDs are correct
- Check you have permissions in the compartment
- Ensure resources exist in the specified region

### Instance Creation Fails

- Check quota limits in your tenancy
- Verify subnet has available IPs
- Ensure shape is available in the region/AD

### SSH Connection Issues

- Wait a few minutes after instance creation for boot
- Verify security list allows SSH (port 22)
- Check instance is in "Running" state
- Verify public IP is assigned

## Cost Optimization

- Use **Always Free** tier shapes if available
- Use **flexible shapes** to scale down when not in use
- Consider **preemptible instances** for cost savings
- Monitor usage and adjust instance size accordingly

## Additional Resources

- [OCI Terraform Provider Documentation](https://registry.terraform.io/providers/oracle/oci/latest/docs)
- [OCI Compute Documentation](https://docs.oracle.com/en-us/iaas/Content/Compute/home.htm)
- [OCI Networking Documentation](https://docs.oracle.com/en-us/iaas/Content/Network/home.htm)

