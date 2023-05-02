locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Data Engineer Zoomcamp Final Project"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "southamerica-east1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "de_project_dataset"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type  = string
  default = "de_project"
}