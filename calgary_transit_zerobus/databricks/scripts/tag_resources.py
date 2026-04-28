"""Tag the ian_preston catalog, ingest schema, and all volumes in that schema with a RemoveAfter date."""

from datetime import date, timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists
from databricks.sdk.service.catalog import EntityTagAssignment

W = WorkspaceClient()

CATALOG = "ian_preston"
SCHEMA = "ingest"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

TAG_KEY = "RemoveAfter"
target = (date.today() + timedelta(days=90)).strftime("%Y-%m-%d")

# Fetch allowed values from the tag policy
policy = next(p for p in W.tag_policies.list_tag_policies() if p.tag_key == TAG_KEY)
allowed = sorted(v.name for v in policy.values)
# Pick the earliest allowed value that is on or after the target date
remove_after = next((v for v in allowed if v >= target), allowed[-1])

print(f"Tagging resources with RemoveAfter: {remove_after}")


def apply_tag(entity_type: str, entity_name: str) -> None:
    tag = EntityTagAssignment(
        entity_type=entity_type,
        entity_name=entity_name,
        tag_key=TAG_KEY,
        tag_value=remove_after,
    )
    try:
        W.entity_tag_assignments.create(tag)
    except AlreadyExists:
        W.entity_tag_assignments.delete(entity_type, entity_name, TAG_KEY)
        W.entity_tag_assignments.create(tag)
    print(f"Tagged {entity_type}: {entity_name}")


# Tag the catalog
apply_tag("catalogs", CATALOG)

# Tag the schema
apply_tag("schemas", FULL_SCHEMA)

# Tag all volumes in the schema
for volume in W.volumes.list(CATALOG, SCHEMA):
    apply_tag("volumes", volume.full_name)

print("Done.")
