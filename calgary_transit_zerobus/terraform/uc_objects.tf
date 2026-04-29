data "databricks_catalog" "default" {
  name = "classic_stable_ptbvhz_ip"
}

resource "databricks_schema" "calgary_transit" {
  catalog_name = data.databricks_catalog.default.id
  name         = "calgary_transit"
  comment      = "Managed by terraform, used for Calgary Transit related data and demos"
}

resource "databricks_sql_endpoint" "tf_endpoint" {
  name                      = "terraform_sql_warehouse"
  cluster_size              = "2X-Small"
  max_num_clusters          = 1
  auto_stop_mins            = 1
  spot_instance_policy      = "COST_OPTIMIZED"
  enable_photon             = false
  enable_serverless_compute = true
  channel {
    name = "CHANNEL_NAME_CURRENT"
  }
}

resource "databricks_sql_table" "calgary_transit_ingest" {
  name         = "calgary_transit_ingest"
  catalog_name = data.databricks_catalog.default.name
  schema_name  = databricks_schema.calgary_transit.name
  table_type   = "MANAGED"
  warehouse_id = databricks_sql_endpoint.tf_endpoint.id
  column {
    name = "update_ts"
    type = "timestamp"
  }
  column {
    name = "data_id"
    type = "string"
  }
  column {
    name = "blob_id"
    type = "string"
  }
  column {
    name = "payload"
    type = "variant"
  }

}
