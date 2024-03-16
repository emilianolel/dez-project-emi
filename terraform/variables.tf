# GENERAL VARIABLES
variable "my_creds" {
  description = "my credentials path"
  default     = "../.secrets/gcp/dez-workspace-emil-063b4716b3be.json"
}

variable "region" {
  description = "Region"
  default     = "us-west4-b"
}

variable "project" {
  description = "Proyect"
  default     = "dez-workspace-emil"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

