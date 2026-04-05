import json
import functions_framework
from google.cloud import bigquery

client = bigquery.Client(project="lc-qas-lake-house-0707")

GOVERNANCE_TABLE = "lc-qas-lake-house-0707.data_governance.table_schema_versions"
REFERENCE_TABLE  = "lc-qas-lake-house-0707.the_guardian.tb_bronze"


def get_latest_snapshot() -> list | None:
    query = f"""
        SELECT schema_snapshot
        FROM `{GOVERNANCE_TABLE}`
        WHERE table_name = '{REFERENCE_TABLE}'
        ORDER BY version_number DESC
        LIMIT 1
    """
    rows = list(client.query(query).result())
    if not rows:
        return None
    return json.loads(rows[0]["schema_snapshot"])


def get_current_schema() -> list:
    table = client.get_table(REFERENCE_TABLE)
    return [
        {
            "name": field.name,
            "mode": field.mode,
            "type": field.field_type,
        }
        for field in table.schema
    ]


def get_next_version() -> int:
    query = f"""
        SELECT COALESCE(MAX(version_number), 0) + 1 AS next_version
        FROM `{GOVERNANCE_TABLE}`
        WHERE table_name = '{REFERENCE_TABLE}'
    """
    rows = list(client.query(query).result())
    return rows[0]["next_version"]


def detect_changes(old: list, new: list) -> tuple[list, list, list]:
    old_map = {f["name"]: f for f in old}
    new_map = {f["name"]: f for f in new}

    added    = [f for name, f in new_map.items() if name not in old_map]
    removed  = [f for name, f in old_map.items() if name not in new_map]
    modified = [
        new_map[name]
        for name, f in new_map.items()
        if name in old_map and f != old_map[name]
    ]
    return added, removed, modified


def insert_new_version(schema: list, change_type: str, description: str):
    next_version = get_next_version()
    snapshot_json = json.dumps(schema).replace("'", "\\'")

    query = f"""
        INSERT INTO `{GOVERNANCE_TABLE}`
          (table_name, version_number, schema_snapshot, changed_at, change_type, description)
        VALUES
        (
          '{REFERENCE_TABLE}',
          {next_version},
          JSON '{snapshot_json}',
          CURRENT_TIMESTAMP(),
          '{change_type}',
          '{description}'
        )
    """
    client.query(query).result()
    print(f"✅ New version {next_version} inserted ({change_type})")


@functions_framework.cloud_event
def detect_schema_changes(cloud_event):
    """Entry point triggered by Cloud Scheduler via Pub/Sub."""
    print(f"🔍 Checking schema for: {REFERENCE_TABLE}")

    try:
        current_schema  = get_current_schema()
        latest_snapshot = get_latest_snapshot()

        # No previous version — insert initial
        if latest_snapshot is None:
            print("No previous version found. Inserting initial schema...")
            insert_new_version(current_schema, "INITIAL", "Initial schema version")
            return

        added, removed, modified = detect_changes(latest_snapshot, current_schema)

        if not added and not removed and not modified:
            print("✅ No schema changes detected.")
            return

        # Build summary
        parts = []
        if added:
            names = ", ".join(f["name"] for f in added)
            print(f"  ➕ Added    : {names}")
            parts.append(f"Added: {names}")

        if removed:
            names = ", ".join(f["name"] for f in removed)
            print(f"  ➖ Removed  : {names}")
            parts.append(f"Removed: {names}")

        if modified:
            names = ", ".join(f["name"] for f in modified)
            print(f"  ✏️  Modified : {names}")
            parts.append(f"Modified: {names}")

        change_type = (
            "ADD_COLUMN"    if added    and not removed and not modified else
            "DROP_COLUMN"   if removed  and not added   and not modified else
            "MODIFY_COLUMN" if modified and not added   and not removed  else
            "MIXED"
        )

        insert_new_version(current_schema, change_type, " | ".join(parts))

    except Exception as e:
        print(f"❌ Error: {e}")
        raise