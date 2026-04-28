resource "databricks_service_principal" "ct_zb_sp" {
  display_name = "calgary-transit-zerobus"
}

resource "databricks_service_principal_secret" "ct_zb_sp_secret" {
  service_principal_id = databricks_service_principal.ct_zb_sp.id
}

resource "local_sensitive_file" "sp_credentials" {
  filename        = "${path.module}/sp_credentials.env"
  content         = <<-EOT
    DATABRICKS_CLIENT_ID=${databricks_service_principal.ct_zb_sp.application_id}
    DATABRICKS_CLIENT_SECRET=${databricks_service_principal_secret.ct_zb_sp_secret.secret}
  EOT
  file_permission = "0600"
}

resource "databricks_grant" "ct_zb_sp_catalog" {
  principal = databricks_service_principal.ct_zb_sp.application_id
  catalog   = data.databricks_catalog.default.id
  privileges = [
    "USE_CATALOG"
  ]
}

resource "databricks_grant" "ct_zb_sp_schema" {
  principal = databricks_service_principal.ct_zb_sp.application_id
  schema    = databricks_schema.calgary_transit.id
  privileges = [
    "USE_SCHEMA"
  ]
}

resource "databricks_grant" "ct_zb_sp_table" {
  principal = databricks_service_principal.ct_zb_sp.application_id
  table     = databricks_sql_table.calgary_transit_ingest.id
  privileges = [
    "MODIFY",
    "SELECT"
  ]
}
