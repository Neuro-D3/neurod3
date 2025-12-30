output "instance_id" {
  description = "OCID of the compute instance"
  value       = oci_core_instance.pr_preview_vm.id
}

output "instance_public_ip" {
  description = "Public IP address of the compute instance"
  value       = oci_core_instance.pr_preview_vm.public_ip
}

output "instance_private_ip" {
  description = "Private IP address of the compute instance"
  value       = oci_core_instance.pr_preview_vm.private_ip
}

output "instance_display_name" {
  description = "Display name of the compute instance"
  value       = oci_core_instance.pr_preview_vm.display_name
}

output "ssh_connection_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh ubuntu@${oci_core_instance.pr_preview_vm.public_ip}"
}

output "security_list_id" {
  description = "OCID of the security list"
  value       = oci_core_security_list.pr_preview_security_list.id
}

output "volume_id" {
  description = "OCID of the additional storage volume (if created)"
  value       = var.create_additional_storage ? oci_core_volume.pr_preview_storage[0].id : null
}

output "vcn_id" {
  description = "OCID of the VCN (created or existing)"
  value       = local.vcn_id
}

output "subnet_id" {
  description = "OCID of the subnet (created or existing)"
  value       = local.subnet_id
}

output "setup_instructions" {
  description = "Instructions for setting up the VM"
  value = <<-EOT
    After the instance is created:
    
    1. SSH into the instance:
       ssh ubuntu@${oci_core_instance.pr_preview_vm.public_ip}
    
    2. Run the setup script:
       sudo ./deploy/oci/setup-vm.sh
    
    3. Configure DNS:
       Point *.preview.<your-domain> to ${oci_core_instance.pr_preview_vm.public_ip}
    
    4. Set up GitHub Actions runner:
       Follow instructions in docs/PR_PREVIEW_SETUP.md
  EOT
}

