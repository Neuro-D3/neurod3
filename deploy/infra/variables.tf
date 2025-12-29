variable "tenancy_ocid" {
  description = "OCID of the tenancy"
  type        = string
}

variable "user_ocid" {
  description = "OCID of the user"
  type        = string
}

variable "fingerprint" {
  description = "Fingerprint of the API key"
  type        = string
  sensitive   = true
}

variable "private_key_path" {
  description = "Path to the private key file"
  type        = string
  sensitive   = true
}

variable "region" {
  description = "OCI region (e.g., us-ashburn-1, us-phoenix-1)"
  type        = string
  default     = "us-ashburn-1"
}

variable "compartment_ocid" {
  description = "OCID of the compartment where resources will be created"
  type        = string
}

variable "vcn_id" {
  description = "OCID of the VCN where the instance will be created"
  type        = string
}

variable "subnet_id" {
  description = "OCID of the subnet where the instance will be created"
  type        = string
}

variable "vcn_cidr" {
  description = "CIDR block of the VCN (for security list rules)"
  type        = string
  default     = "10.0.0.0/16"
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

