variable "use_cli_config" {
  description = "Use OCI CLI config file for authentication (set to false for Cloud Shell)"
  type        = bool
  default     = false
}

variable "cli_config_profile" {
  description = "OCI CLI config profile to use (default: DEFAULT)"
  type        = string
  default     = "DEFAULT"
}

variable "tenancy_ocid" {
  description = "OCID of the tenancy (get it in Cloud Shell with: grep '^tenancy=' ~/.oci/config | cut -d'=' -f2)"
  type        = string
  default     = ""
  # In Cloud Shell, run: grep "^tenancy=" ~/.oci/config | cut -d'=' -f2
  # Or find it in: OCI Console > Administration > Tenancy Details
}

variable "user_ocid" {
  description = "OCID of the user (required if use_cli_config is false)"
  type        = string
  default     = ""
}

variable "fingerprint" {
  description = "Fingerprint of the API key (required if use_cli_config is false)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "private_key_path" {
  description = "Path to the private key file (required if use_cli_config is false)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "region" {
  description = "OCI region (e.g., us-ashburn-1, us-phoenix-1)"
  type        = string
  default     = "us-sanjose-1"
}

variable "compartment_ocid" {
  description = "OCID of the compartment where resources will be created (defaults to tenancy if not specified)"
  type        = string
  default     = ""
}

variable "vcn_id" {
  description = "OCID of existing VCN (leave empty to create new VCN)"
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "OCID of existing subnet (leave empty to create new subnet)"
  type        = string
  default     = ""
}

variable "vcn_cidr" {
  description = "CIDR block of the VCN (used when creating new VCN)"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr" {
  description = "CIDR block of the subnet (used when creating new subnet)"
  type        = string
  default     = "10.0.1.0/24"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "neurod3"
}

variable "instance_shape" {
  description = "Shape of the compute instance"
  type        = string
  default     = "VM.Standard.E4.Flex"
}

variable "image_id" {
  description = "OCID of the image to use (optional - will auto-detect Ubuntu 22.04 if not provided)"
  type        = string
  default     = ""
}

variable "availability_domain" {
  description = "Availability domain name (optional - will auto-detect if not provided, e.g., 'AD-1', 'us-sanjose-1-AD-1')"
  type        = string
  default     = ""
}

variable "instance_ocpus" {
  description = "Number of OCPUs for the instance"
  type        = number
  default     = 2
}

variable "instance_memory_gb" {
  description = "Memory in GB for the instance"
  type        = number
  default     = 8
}

variable "ssh_public_key" {
  description = "SSH public key for the instance"
  type        = string
  sensitive   = true
}

variable "create_additional_storage" {
  description = "Whether to create additional block storage"
  type        = bool
  default     = false
}

variable "additional_storage_size_gb" {
  description = "Size of additional storage in GB"
  type        = number
  default     = 100
}

variable "storage_vpus_per_gb" {
  description = "VPUs per GB for block storage (0, 10, 20)"
  type        = number
  default     = 10
}

