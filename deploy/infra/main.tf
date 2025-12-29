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
  tenancy_ocid     = var.tenancy_ocid
  user_ocid        = var.user_ocid
  fingerprint      = var.fingerprint
  private_key_path = var.private_key_path
  region           = var.region
}

# Get availability domains
data "oci_identity_availability_domains" "ads" {
  compartment_id = var.tenancy_ocid
}

# Get the first availability domain
data "oci_identity_availability_domain" "ad" {
  compartment_id = var.tenancy_ocid
  ad_number      = 1
}

# Get compartment
data "oci_identity_compartment" "compartment" {
  id = var.compartment_ocid
}

# Get VCN
data "oci_core_vcn" "vcn" {
  vcn_id = var.vcn_id
}

# Get subnet
data "oci_core_subnet" "subnet" {
  subnet_id = var.subnet_id
}

# Get image for Ubuntu
data "oci_core_images" "ubuntu_images" {
  compartment_id           = var.compartment_ocid
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
  compartment_id = var.compartment_ocid
  vcn_id         = var.vcn_id
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
  compartment_id      = var.compartment_ocid
  availability_domain = data.oci_identity_availability_domain.ad.name
  display_name         = "${var.project_name}-pr-preview-vm"
  shape                = var.instance_shape

  shape_config {
    ocpus         = var.instance_ocpus
    memory_in_gbs = var.instance_memory_gb
  }

  create_vnic_details {
    subnet_id        = var.subnet_id
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

  compartment_id      = var.compartment_ocid
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

