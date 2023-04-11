locals {
  data_lake_bucket = "refereeing_data_lake"
}

variable "project" {
  description = "Refereeing Appointments Data"
  default = "refereeinganalysis"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "australia-southeast1"
  type = string
}

variable "storage_class" {
  description = "Our data lake bucket Prefect will store data in"
  default = "STANDARD"
}

variable "BQ_DATASET_1" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "appointments_data_raw"
}

variable "BQ_DATASET_2" {
  description = "BigQuery Dataset that will be the transformed layer of the data warehouse. Will ingest from the raw layer"
  type = string
  default = "appointments_data_transformed"
}
