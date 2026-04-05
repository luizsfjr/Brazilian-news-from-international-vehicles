import json
import base64
import requests
import functions_framework
from google.cloud import bigquery
from google.cloud import secretmanager

PROJECT_ID       = "lc-qas-lake-house-0707"
GOVERNANCE_TABLE = f"{PROJECT_ID}.data_governance.table_schema_versions"
REFERENCE_TABLE  = f"{PROJECT_ID}.the_guardian.tb_bronze"
GITHUB_REPO      = "luizsfjr/Brazilian-news-from-international-vehicles"
SILVER_FILE_PATH = "definitions/tb_silver.sqlx"
BASE_BRANCH      = "main"

client = bigquery.Client(project=PROJECT_ID)


# ── Secrets ──────────────────────────────────────────────────────────────────

def get_github_token() -> str:
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/github-token/versions/latest"
    return sm.access_secret_version(request={"name": name}).payload.data.decode()


# ── BigQuery helpers ──────────────────────────────────────────────────────────

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
    snapshot = rows[0]["schema_snapshot"]
    return snapshot if isinstance(snapshot, list) else json.loads(snapshot)


def get_live_schema() -> list:
    table = client.get_table(REFERENCE_TABLE)
    return [
        {"name": field.name, "mode": field.mode, "type": field.field_type}
        for field in table.schema
    ]


def get_next_version() -> int:
    query = f"""
        SELECT COALESCE(MAX(version_number), 0) + 1 AS next_version
        FROM `{GOVERNANCE_TABLE}`
        WHERE table_name = '{REFERENCE_TABLE}'
    """
    return list(client.query(query).result())[0]["next_version"]


def detect_changes(old: list, new: list) -> tuple[list, list, list]:
    old_map = {f["name"]: f for f in old}
    new_map = {f["name"]: f for f in new}
    added    = [f for name, f in new_map.items() if name not in old_map]
    removed  = [f for name, f in old_map.items() if name not in new_map]
    modified = [new_map[name] for name, f in new_map.items()
                if name in old_map and f != old_map[name]]
    return added, removed, modified


def insert_new_version(schema: list, change_type: str, description: str):
    next_version  = get_next_version()
    snapshot_json = json.dumps(schema).replace("'", "\\'")
    query = f"""
        INSERT INTO `{GOVERNANCE_TABLE}`
          (table_name, version_number, schema_snapshot, changed_at, change_type, description)
        VALUES (
          '{REFERENCE_TABLE}',
          {next_version},
          JSON '{snapshot_json}',
          CURRENT_TIMESTAMP(),
          '{change_type}',
          '{description}'
        )
    """
    client.query(query).result()
    print(f"✅ Version {next_version} inserted ({change_type})")
    return next_version


# ── GitHub helpers ────────────────────────────────────────────────────────────

def github_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }


def get_base_sha(token: str) -> str:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{BASE_BRANCH}"
    res = requests.get(url, headers=github_headers(token))
    res.raise_for_status()
    return res.json()["object"]["sha"]


def create_branch(token: str, branch: str, sha: str):
    url  = f"https://api.github.com/repos/{GITHUB_REPO}/git/refs"
    body = {"ref": f"refs/heads/{branch}", "sha": sha}
    res  = requests.post(url, headers=github_headers(token), json=body)
    res.raise_for_status()
    print(f"✅ Branch created: {branch}")


def get_file(token: str, branch: str) -> tuple[str, str]:
    """Returns (content, sha) of the silver SQLX file."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{SILVER_FILE_PATH}"
    res = requests.get(url, headers=github_headers(token), params={"ref": branch})
    res.raise_for_status()
    data    = res.json()
    content = base64.b64decode(data["content"]).decode()
    return content, data["sha"]


def patch_sqlx(content: str, added: list, removed: list, modified: list, version: int) -> str:
    """
    Since silver uses SELECT *, the SQLX logic stays the same.
    We update the assertions block to flag new non-null candidates
    and add a schema change comment for reviewer awareness.
    """
    lines = content.splitlines()

    # Build change summary comment
    summary_lines = [f"// schema-evolution: v{version}"]
    if added:
        cols = ", ".join(f["name"] for f in added)
        summary_lines.append(f"// Added columns   : {cols}")
    if removed:
        cols = ", ".join(f["name"] for f in removed)
        summary_lines.append(f"// Removed columns : {cols}")
    if modified:
        cols = ", ".join(f["name"] for f in modified)
        summary_lines.append(f"// Modified columns: {cols}")
    summary_lines.append("// Review: update assertions/docs if needed.")
    comment_block = "\n".join(summary_lines)

    # Insert comment right after the closing brace of the config block
    result   = []
    in_config = False
    brace_depth = 0
    inserted  = False

    for line in lines:
        result.append(line)
        if not inserted:
            if "config {" in line:
                in_config   = True
                brace_depth = 1
            elif in_config:
                brace_depth += line.count("{") - line.count("}")
                if brace_depth == 0:
                    result.append("")
                    result.append(comment_block)
                    result.append("")
                    in_config = False
                    inserted  = True

    return "\n".join(result)


def commit_file(token: str, branch: str, content: str, file_sha: str, message: str):
    url  = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{SILVER_FILE_PATH}"
    body = {
        "message": message,
        "content": base64.b64encode(content.encode()).decode(),
        "sha":     file_sha,
        "branch":  branch
    }
    res = requests.put(url, headers=github_headers(token), json=body)
    res.raise_for_status()
    print("✅ File committed")


def open_pr(token: str, branch: str, title: str, body: str) -> str:
    url  = f"https://api.github.com/repos/{GITHUB_REPO}/pulls"
    data = {
        "title": title,
        "body":  body,
        "head":  branch,
        "base":  BASE_BRANCH
    }
    res = requests.post(url, headers=github_headers(token), json=data)
    res.raise_for_status()
    pr_url = res.json()["html_url"]
    print(f"✅ PR opened: {pr_url}")
    return pr_url


def build_pr_body(added: list, removed: list, modified: list, version: int) -> str:
    lines = [
        "## Schema evolution detected",
        f"**Table:** `{REFERENCE_TABLE}`  ",
        f"**New version:** `v{version}`",
        "",
        "### Changes",
    ]
    if added:
        lines.append("**Added columns:**")
        for f in added:
            lines.append(f"- `{f['name']}` ({f['type']}, {f['mode']})")
    if removed:
        lines.append("**Removed columns:**")
        for f in removed:
            lines.append(f"- `{f['name']}` ({f['type']})")
    if modified:
        lines.append("**Modified columns:**")
        for f in modified:
            lines.append(f"- `{f['name']}` → {f['type']}, {f['mode']}")
    lines += [
        "",
        "### Action required",
        "- [ ] Review if new columns should be added to `nonNull` assertions",
        "- [ ] Update column documentation if needed",
        "- [ ] Confirm Dataform compiles cleanly after merge",
        "",
        "_This PR was opened automatically by the schema evolution detector._"
    ]
    return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

@functions_framework.cloud_event
def detect_schema_changes(cloud_event):
    print(f"🔍 Checking schema for: {REFERENCE_TABLE}")

    try:
        live_schema     = get_live_schema()
        latest_snapshot = get_latest_snapshot()

        if latest_snapshot is None:
            print("No previous version found. Inserting initial schema...")
            insert_new_version(live_schema, "INITIAL", "Initial schema version")
            return

        added, removed, modified = detect_changes(latest_snapshot, live_schema)

        if not added and not removed and not modified:
            print("✅ No schema changes detected.")
            return

        # Build description
        parts = []
        if added:    parts.append("Added: "    + ", ".join(f["name"] for f in added))
        if removed:  parts.append("Removed: "  + ", ".join(f["name"] for f in removed))
        if modified: parts.append("Modified: " + ", ".join(f["name"] for f in modified))

        change_type = (
            "ADD_COLUMN"    if added    and not removed and not modified else
            "DROP_COLUMN"   if removed  and not added   and not modified else
            "MODIFY_COLUMN" if modified and not added   and not removed  else
            "MIXED"
        )

        # 1. Save to governance table
        version = insert_new_version(live_schema, change_type, " | ".join(parts))

        # 2. GitHub PR flow
        token      = get_github_token()
        base_sha   = get_base_sha(token)
        branch     = f"schema-evolution/v{version}-{change_type.lower()}"

        create_branch(token, branch, base_sha)

        content, file_sha = get_file(token, branch)
        patched           = patch_sqlx(content, added, removed, modified, version)

        commit_file(
            token, branch, patched, file_sha,
            f"chore: schema evolution v{version} - {change_type}"
        )

        pr_url = open_pr(
            token,
            branch,
            title=f"[Schema Evolution] {change_type} - v{version}",
            body=build_pr_body(added, removed, modified, version)
        )

        print(f"🎉 Done! PR: {pr_url}")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise