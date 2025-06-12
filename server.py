"""
Databricks Explorer MCP – SQL-enabled via search/fetch only
(warehouse fixed, query ID = 'query::<sql>')
"""

from fastmcp import FastMCP
import os, requests, time, typing as t
import logging

# -------------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("databricks_explorer")


# -------------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------------
WORKSPACE_URL  = os.getenv("DATABRICKS_WORKSPACE_URL")
ACCESS_TOKEN   = os.getenv("DATABRICKS_TOKEN")
WAREHOUSE_ID   = os.getenv("DATABRICKS_WAREHOUSE_ID")          # *** REQUIRED ***

if not (WORKSPACE_URL and ACCESS_TOKEN and WAREHOUSE_ID):
    raise RuntimeError(
        "Set DATABRICKS_WORKSPACE_URL, DATABRICKS_TOKEN and DATABRICKS_WAREHOUSE_ID"
    )

HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

def _get(path: str, *, timeout: int = 10) -> dict:
    r = requests.get(f"{WORKSPACE_URL}{path}", headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()

# -------------------------------------------------------------------------
# Unity-Catalog helpers
# -------------------------------------------------------------------------
def list_catalogs() -> dict: return _get("/api/2.1/unity-catalog/catalogs")
def list_schemas(cat:str)->dict: return _get(f"/api/2.1/unity-catalog/schemas?catalog_name={cat}")
def list_tables(cat:str, sch:str)->dict:
    return _get(f"/api/2.1/unity-catalog/tables?catalog_name={cat}&schema_name={sch}", timeout=60)

# -------------------------------------------------------------------------
# SQL helper (always same warehouse)
# -------------------------------------------------------------------------
def _run_sql(sql: str, wait: int = 15) -> dict:
    payload = {
        "statement":     sql,
        "warehouse_id":  WAREHOUSE_ID,
        "wait_timeout":  f"{wait}s",
        "on_wait_timeout": "CONTINUE",
        "disposition":   "INLINE",
        "format":        "JSON_ARRAY",
    }
    r = requests.post(
        f"{WORKSPACE_URL}/api/2.0/sql/statements/",
        headers={**HEADERS, "Content-Type": "application/json"},
        json=payload, timeout=10
    )
    r.raise_for_status()
    meta = r.json()
    st_id = meta.get("statement_id")

    for _ in range(wait):
        status = _get(f"/api/2.0/sql/statements/{st_id}")
        state  = status["status"]["state"]
        if state == "SUCCEEDED" and status.get("result"):
            return status  # Return the whole status dict, not just 'result'
        if state == "FAILED":
            error_msg = status["status"].get("error", {}).get("message", str(status))
            raise RuntimeError(f"SQL execution failed: {error_msg}")
        time.sleep(1)
    raise TimeoutError("SQL timed out after wait_timeout")

# -------------------------------------------------------------------------
# MCP definition
# -------------------------------------------------------------------------
mcp = FastMCP(
    name="Databricks Explorer MCP",
    instructions=(
        "Use `search` to find Unity Catalog objects *or* to prepare a SQL query.\n\n"
        "If the search text looks like SQL (or starts with `sql:`) the tool returns "
        "a stub result whose ID is literally `query::<sql>`.\n"
        "Call `fetch` with that ID to execute the statement in the fixed warehouse.\n\n"
        "ID formats:\n"
        "  • catalog::<catalog>\n"
        "  • schema::<catalog>.<schema>\n"
        "  • table::<catalog>.<schema>.<table>\n"
        "  • query::<SQL statement>\n"
    ),
)

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------
def _is_sql(text: str) -> bool:
    stripped = text.lstrip().lower()
    return stripped.startswith(("select", "with", "insert", "update", "delete", "merge")) \
        or stripped.startswith("sql:")

# -------------------------------------------------------------------------
# search
# -------------------------------------------------------------------------
@mcp.tool()
async def search(query: str) -> dict:
    """
    • Normal text  -> metadata search.  
    • SQL string   -> one stub: id = 'query::<sql>'.
    """
    results: t.List[dict] = []

    # ---- SQL path -------------------------------------------------------
    if _is_sql(query):
        sql = query[len("sql:"):].strip() if query.lower().startswith("sql:") else query.strip()
        results.append({
            "id":     f"query::{sql}",
            "title":  "SQL query preview",
            "text":   (sql[:60] + "…") if len(sql) > 60 else sql,
            "metadata": {"sql": sql},
        })
        return {"results": results}

    # ---- Metadata search path ------------------------------------------
    qlc = query.lower()
    try:
        cats = list_catalogs()
        for cat in cats.get("catalogs", []):
            if qlc in cat["name"].lower():
                results.append({
                    "id": f"catalog::{cat['name']}",
                    "title": f"Catalog: {cat['name']}",
                    "text": cat.get("comment", ""),
                    "metadata": cat,
                })
        for cat in cats.get("catalogs", []):
            schs = list_schemas(cat["name"])
            for sch in schs.get("schemas", []):
                fq = f"{cat['name']}.{sch['name']}"
                if qlc in fq.lower():
                    results.append({
                        "id": f"schema::{fq}",
                        "title": f"Schema: {fq}",
                        "text": sch.get("comment", ""),
                        "metadata": sch,
                    })
                tbls = list_tables(cat["name"], sch["name"])
                for tbl in tbls.get("tables", []):
                    fqt = f"{fq}.{tbl['name']}"
                    if qlc in fqt.lower():
                        results.append({
                            "id": f"table::{fqt}",
                            "title": f"Table: {fqt}",
                            "text": tbl.get("comment", ""),
                            "metadata": tbl,
                        })
    except Exception as exc:
        results.append({"error": str(exc)})

    return {"results": results}

# -------------------------------------------------------------------------
# fetch
# -------------------------------------------------------------------------
@mcp.tool()
async def fetch(rid: str) -> dict:
    """
    • query::<sql>      -> run SQL, return rows.  
    • catalog/…/table   -> return metadata.
    """
    # ---- SQL execution --------------------------------------------------
    if rid.startswith("query::"):
        sql = rid.split("::", 1)[1]           # everything after first '::'
        try:
            res = _run_sql(sql)
            return {
                "id": rid,
                "title": "SQL query result",
                "text":  f"Returned {res['manifest']['total_row_count']} row(s)",
                "metadata": res,
            }
        except Exception as exc:
            return {"error": f"SQL execution failed: {exc}"}

    # ---- Metadata fetch -------------------------------------------------
    try:
        kind, name = rid.split("::", 1)
    except ValueError:
        return {"error": "Bad ID format"}

    try:
        if kind == "catalog":
            for cat in list_catalogs().get("catalogs", []):
                if cat["name"] == name:
                    return {"id": rid, "title": f"Catalog: {name}", "text": cat.get("comment",""), "metadata": cat}
        elif kind == "schema":
            cat, sch = name.split(".", 1)
            for s in list_schemas(cat).get("schemas", []):
                if s["name"] == sch:
                    return {"id": rid, "title": f"Schema: {name}", "text": s.get("comment",""), "metadata": s}
        elif kind == "table":
            cat, sch, tbl = name.split(".", 2)
            for t in list_tables(cat, sch).get("tables", []):
                if t["name"] == tbl:
                    return {"id": rid, "title": f"Table: {name}", "text": t.get("comment",""), "metadata": t}
        return {"error": f"Resource '{rid}' not found"}
    except Exception as exc:
        return {"error": str(exc)}

# -------------------------------------------------------------------------
# entrypoint
# -------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(
        transport="sse",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080)),
        path="/sse"
    )
