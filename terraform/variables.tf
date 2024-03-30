# GENERAL VARIABLES
variable "my_creds" {
  description = "my credentials path"
  default     = "../.secrets/gcp/dez-workspace-emil-063b4716b3be.json" # Change 
}

variable "region" {
  description = "Region"
  default     = "us-west4"
}

variable "zone" {
  description = "Zone"
  default     = "us-west4-b"
}

variable "project" {
  description = "Proyect"
  default     = "dez-workspace-emil" # Change
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "static_ip_address_name" {
    description = "Static IP Address Name"
    default = "my-static-ip-address"
}

variable "service_account_email" {
    description = "Service Account Email"
    default = "dez-project-emil@dez-workspace-emil.iam.gserviceaccount.com" # Change
}

variable "compute_instance_name" {
    description = "Compute Instance Name"
    default = "terraform-test-instance"
}

variable "ssh_username" {
    description = "SSH Username"
    default = "emilel" # Change
}

variable "ssh_pub_key" {
    description = "Path to pub key"
    default = "../.ssh/gcp.pub" # Change
}

variable "landing_bucket_name" {
    description = "Landing Bucket Name"
    default = "landing-bucket"
}

variable "parquet_bucket_name" {
    description = "Landing Bucket Name"
    default = "parquet-bucket"
}

variable "dataproc_bucket_name" {
    description = "Dataproc Bucket Name"
    default = "dataproc-staging-bucket-dez-test"
}

variable "geo_dataset_name" {
    description = "Geographical Dataset Name"
    default = "raw_geo_mx_test"
}

variable "crimes_dataset_name" {
    description = "Crimes Dataset Name"
    default = "raw_dez_crimes_test"
}

variable "mx_crimes_dataset_name" {
    description = "Fact MX Dataset Name"
    default = "dez_project_mx_crimes_test"
}

variable "dataproc_cluster_name" {
    description = "Dataproc Cluster Name"
    default = "dez-project-cluster"
}
