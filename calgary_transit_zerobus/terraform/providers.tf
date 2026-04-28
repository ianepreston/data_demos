terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.113.0"
    }
  }
}
provider "databricks" {
  profile = "fevm-az"
}
