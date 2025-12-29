terraform {
  required_version = ">= 1.0"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 5.0"
    }
  }
}

provider "oci" {
  # Cloud Shell automatically uses DEFAULT profile from ~/.oci/config
  # No authentication variables needed when running from Cloud Shell!
  config_file_profile = var.use_cli_config ? var.cli_config_profile : null
  tenancy_ocid        = var.use_cli_config ? null : var.tenancy_ocid
  user_ocid           = var.use_cli_config ? null : var.user_ocid
  fingerprint         = var.use_cli_config ? null : var.fingerprint
  private_key_path     = var.use_cli_config ? null : var.private_key_path
  region               = var.region
}

# Get tenancy OCID - in Cloud Shell, get it easily with OCI CLI
locals {
  # In Cloud Shell, users can get tenancy_ocid with: oci iam tenancy get --query 'data.id' --raw-output
  tenancy_ocid = var.tenancy_ocid
}

# Get availability domains
data "oci_identity_availability_domains" "ads" {
  compartment_id = local.tenancy_ocid
}

# Get the first availability domain
data "oci_identity_availability_domain" "ad" {
  compartment_id = local.tenancy_ocid
  ad_number      = 1
}

# Get compartment (use tenancy if compartment not specified)
data "oci_identity_compartment" "compartment" {
  id = var.compartment_ocid != "" ? var.compartment_ocid : local.tenancy_ocid
}

# Use existing VCN or create new one
data "oci_core_vcn" "existing_vcn" {
  count  = var.vcn_id != "" ? 1 : 0
  vcn_id = var.vcn_id
}

# Create VCN if not provided
resource "oci_core_vcn" "vcn" {
  count = var.vcn_id == "" ? 1 : 0

  compartment_id = local.compartment_id
  cidr_blocks    = [var.vcn_cidr]
  display_name   = "${var.project_name}-pr-preview-vcn"
  dns_label      = "${replace(var.project_name, "-", "")}prpreview"

  freeform_tags = {
    "Project"   = var.project_name
    "ManagedBy" = "Terraform"
  }
}

# Get internet gateway for VCN
data "oci_core_internet_gateways" "existing_igw" {
  count   = var.vcn_id != "" ? 1 : 0
  vcn_id  = var.vcn_id
  state   = "AVAILABLE"
}

# Create internet gateway if VCN is new
resource "oci_core_internet_gateway" "igw" {
  count = var.vcn_id == "" ? 1 : 0

  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.vcn[0].id
  display_name   = "${var.project_name}-pr-preview-igw"
  enabled        = true

  freeform_tags = {
    "Project"   = var.project_name
    "ManagedBy" = "Terraform"
  }
}

# Get default route table
data "oci_core_route_tables" "existing_route_table" {
  count  = var.vcn_id != "" ? 1 : 0
  vcn_id = var.vcn_id
}

# Create default route table with internet gateway route
resource "oci_core_default_route_table" "route_table" {
  count = var.vcn_id == "" ? 1 : 0

  manage_default_resource_id = oci_core_vcn.vcn[0].default_route_table_id

  route_rules {
    network_entity_id = oci_core_internet_gateway.igw[0].id
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
  }
}

# Use existing subnet or create new one
data "oci_core_subnet" "existing_subnet" {
  count     = var.subnet_id != "" ? 1 : 0
  subnet_id = var.subnet_id
}

# Create subnet if not provided
resource "oci_core_subnet" "subnet" {
  count = var.subnet_id == "" ? 1 : 0

  compartment_id    = local.compartment_id
  vcn_id            = local.vcn_id
  cidr_block        = var.subnet_cidr
  display_name       = "${var.project_name}-pr-preview-subnet"
  dns_label          = "${replace(var.project_name, "-", "")}prpreview"
  security_list_ids  = [oci_core_security_list.pr_preview_security_list.id]
  route_table_id     = var.vcn_id != "" ? data.oci_core_route_tables.existing_route_table[0].route_tables[0].id : oci_core_vcn.vcn[0].default_route_table_id

  freeform_tags = {
    "Project"   = var.project_name
    "ManagedBy" = "Terraform"
  }
}

# Local values for VCN and subnet IDs
locals {
  vcn_id         = var.vcn_id != "" ? var.vcn_id : oci_core_vcn.vcn[0].id
  subnet_id      = var.subnet_id != "" ? var.subnet_id : oci_core_subnet.subnet[0].id
  compartment_id = var.compartment_ocid != "" ? var.compartment_ocid : local.tenancy_ocid
}

# Get image for Ubuntu
data "oci_core_images" "ubuntu_images" {
  compartment_id           = local.compartment_id
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "22.04"
  shape                    = var.instance_shape
  sort_by                 = "TIMECREATED"
  sort_order              = "DESC"
}

# Get shape details
data "oci_core_shape" "shape" {
  compartment_id      = var.compartment_ocid
  availability_domain = data.oci_identity_availability_domain.ad.name
  name                = var.instance_shape
}

# Create security list for PR preview VM
resource "oci_core_security_list" "pr_preview_security_list" {
  compartment_id = local.compartment_id
  vcn_id         = local.vcn_id
  display_name   = "${var.project_name}-pr-preview-security-list"

  # Allow SSH
  ingress_security_rules {
    protocol    = "6" # TCP
    source      = "0.0.0.0/0"
    description = "Allow SSH from anywhere"

    tcp_options {
      min = 22
      max = 22
    }
  }

  # Allow HTTP
  ingress_security_rules {
    protocol    = "6" # TCP
    source      = "0.0.0.0/0"
    description = "Allow HTTP from anywhere"

    tcp_options {
      min = 80
      max = 80
    }
  }

  # Allow HTTPS
  ingress_security_rules {
    protocol    = "6" # TCP
    source      = "0.0.0.0/0"
    description = "Allow HTTPS from anywhere"

    tcp_options {
      min = 443
      max = 443
    }
  }

  # Allow Caddy admin API (consider restricting to localhost/VCN)
  ingress_security_rules {
    protocol    = "6" # TCP
    source      = var.vcn_cidr
    description = "Allow Caddy admin API from VCN"

    tcp_options {
      min = 2019
      max = 2019
    }
  }

  # Egress rules - allow all outbound
  egress_security_rules {
    protocol    = "all"
    destination = "0.0.0.0/0"
    description = "Allow all outbound traffic"
  }
}

# Create compute instance
resource "oci_core_instance" "pr_preview_vm" {
  compartment_id      = local.compartment_id
  availability_domain = data.oci_identity_availability_domain.ad.name
  display_name         = "${var.project_name}-pr-preview-vm"
  shape                = var.instance_shape

  shape_config {
    ocpus         = var.instance_ocpus
    memory_in_gbs = var.instance_memory_gb
  }

  create_vnic_details {
    subnet_id        = local.subnet_id
    assign_public_ip = true
    display_name     = "${var.project_name}-pr-preview-vnic"
    hostname_label   = "${var.project_name}-pr-preview"
  }

  source_details {
    source_type = "image"
    source_id   = data.oci_core_images.ubuntu_images.images[0].id
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
    user_data = base64encode(templatefile("${path.module}/user-data.sh", {
      project_name = var.project_name
    }))
  }

  preserve_boot_volume = false

  freeform_tags = {
    "Project"     = var.project_name
    "Environment" = "pr-preview"
    "ManagedBy"   = "Terraform"
  }
}

# Create block volume for additional storage (optional)
resource "oci_core_volume" "pr_preview_storage" {
  count = var.create_additional_storage ? 1 : 0

  compartment_id      = local.compartment_id
  availability_domain = data.oci_identity_availability_domain.ad.name
  display_name         = "${var.project_name}-pr-preview-storage"
  size_in_gbs         = var.additional_storage_size_gb
  vpus_per_gb         = var.storage_vpus_per_gb

  freeform_tags = {
    "Project"     = var.project_name
    "Environment" = "pr-preview"
    "ManagedBy"   = "Terraform"
  }
}

# Attach block volume to instance (optional)
resource "oci_core_volume_attachment" "pr_preview_storage_attachment" {
  count = var.create_additional_storage ? 1 : 0

  attachment_type = "paravirtualized"
  instance_id     = oci_core_instance.pr_preview_vm.id
  volume_id       = oci_core_volume.pr_preview_storage[0].id
  display_name    = "${var.project_name}-storage-attachment"
}

