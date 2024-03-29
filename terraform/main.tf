terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  credentials = file(var.my_creds)
  project     = var.project
  region      = var.region
}

resource "google_compute_address" "static-ip-address" {
  name = var.static_ip_address_name
}

resource "google_compute_instance" "terraform-test-intance" {
  boot_disk {
    auto_delete = true
    device_name = var.compute_instance_name 

    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20240307b"
      size  = 100
      type  = "pd-balanced"
    }

    mode = "READ_WRITE"
  }

  can_ip_forward      = false
  deletion_protection = false
  enable_display      = false

  labels = {
    goog-ec-src = "vm_add-tf"
  }

  machine_type = "e2-standard-4"
  name         = var.compute_instance_name 

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.static-ip-address.address
    }
  }

  metadata = {
    ssh-keys = "${var.ssh_username}:${file(var.ssh_pub_key)}"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  service_account {
    email  = var.service_account_email # set variable
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }

  zone = var.zone
}

resource "random_id" "bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "landing_bucket" {
  name          = format("%s-%s", var.landing_bucket_name, "${random_id.bucket_prefix.hex}")
  location      = "US"
  force_destroy = true
}

resource "google_storage_bucket" "parquet_bucket" {
  name          = format("%s-%s", var.parquet_bucket_name, "${random_id.bucket_prefix.hex}")
  location      = "US"
  force_destroy = true
}

resource "google_storage_bucket" "dataproc_bucket" {
  name          = format("%s-%s", var.dataproc_bucket_name, "${random_id.bucket_prefix.hex}")
  location      = "US"
  force_destroy = true
}

resource "google_bigquery_dataset" "raw_geo_mx" {
  dataset_id                  = var.geo_dataset_name
  access {
    role          = "OWNER"
    user_by_email = var.service_account_email
  }
}

resource "google_bigquery_dataset" "raw_dez_crimes" {
  dataset_id                  = var.crimes_dataset_name
  access {
    role          = "OWNER"
    user_by_email = var.service_account_email
  }
}
resource "google_bigquery_dataset" "dez_project_mx_crimes" {
  dataset_id                  = var.mx_crimes_dataset_name
  access {
    role          = "OWNER"
    user_by_email = var.service_account_email
  }
}

resource "google_dataproc_cluster" "dataproc_cluster" {
  name     = var.dataproc_cluster_name
  region   = "us-central1"
  graceful_decommission_timeout = "120s"
  
  labels = {
    env = "dev"
    foo = "bar"
  }

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_bucket.name

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-2"
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances    = 2
      machine_type     = "e2-standard-4"
      disk_config {
        boot_disk_size_gb = 30
        num_local_ssds    = 0
      }
    }

    preemptible_worker_config {
      num_instances = 0
    }

    # Override or set some custom properties
    software_config {
      image_version = "2.1.43-ubuntu20"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

  }
  depends_on = [
        google_storage_bucket.dataproc_bucket
    ]
}
