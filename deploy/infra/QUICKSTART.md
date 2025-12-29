# Quick Start - Cloud Shell

## 1. Get Your Tenancy OCID
```bash
oci iam tenancy get --query 'data.id' --raw-output
```
Copy the output.

## 2. Generate SSH Key
```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/oci_key
cat ~/.ssh/oci_key.pub
```
Copy the output.

## 3. Configure Terraform
```bash
cd deploy/infra
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars
```

Fill in just these 3 values:
```hcl
region = "us-ashburn-1"  # Your region
tenancy_ocid = "ocid1.tenancy.oc1..aaaaaaa..."  # From step 1
ssh_public_key = "ssh-ed25519 AAAAC3..."  # From step 2
```

Save and exit (Ctrl+X, Y, Enter).

## 4. Run Terraform
```bash
terraform init
terraform apply
```
Type `yes` when prompted.

## 5. Get Instance IP
```bash
terraform output instance_public_ip
```

## 6. SSH to Instance
```bash
ssh -i ~/.ssh/oci_key ubuntu@<ip-from-step-5>
```

Done! Now run the setup script on the VM.

