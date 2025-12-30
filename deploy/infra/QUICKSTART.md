# Quick Start - Cloud Shell

## 1. Get Your Tenancy OCID

**Option A - From OCI Console:**
1. Go to **Administration** > **Tenancy Details**
2. Copy the **OCID** (starts with `ocid1.tenancy.oc1..`)

**Option B - From Cloud Shell (if config exists):**
```bash
grep "^tenancy=" ~/.oci/config 2>/dev/null | cut -d'=' -f2 || echo "Config not found - use Option A"
```

**Option C - From any OCI resource:**
```bash
oci iam compartment list --all --compartment-id-in-subtree true --limit 1 --query 'data[0].compartment-id' --raw-output
```

## 2. Generate SSH Key
```bash
ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/oci_key
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
ssh_public_key = "ssh-rsa AAAAB3..."  # From step 2
```

Save and exit (Ctrl+X, Y, Enter).

**Note:** If you already edited `terraform.tfvars.example`, just rename it:
```bash
mv terraform.tfvars.example terraform.tfvars
```

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

