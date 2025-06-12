# Databricks Explorer MCP

**Databricks Explorer MCP** is a prototype Minimal Command Protocol (MCP) interface designed for deep research and interactive data exploration via ChatGPT. It allows metadata exploration and SQL execution in a Databricks environment using a simplified search/fetch mechanism.

> ⚠️ **Note:** This is a conceptual example designed for future integration with ChatGPT. While the structure is functional in a server-hosted context, **ChatGPT cannot directly execute SQL or external fetch requests** yet. This MCP serves as a foundation for such future capabilities.

---

## 💡 Purpose

This tool demonstrates how ChatGPT could eventually support real-time, SQL-driven research through simple commands. By building a unified abstraction for metadata discovery and SQL querying, it bridges the gap between conversational interfaces and data platforms like Databricks.

---

## 🚀 Features

- 🔍 **Search Tool**:
  - Discover catalogs, schemas, and tables via keyword search.
  - Detects SQL-like input and creates a placeholder `query::<sql>` ID.

- 💥 **Fetch Tool**:
  - Executes SQL queries using a fixed warehouse (only in external environments).
  - Returns Unity Catalog metadata for catalog/schema/table IDs.

- 🧠 **SQL-aware Search**:
  - Input beginning with `sql:` or SQL verbs (SELECT, INSERT, etc.) is interpreted as a query stub.

- 🌐 **FastMCP Compatible**:
  - Built on the FastMCP framework.
  - Supports SSE transport for interactive use.

---

## ⚙️ Environment Variables

| Variable                   | Description                              |
|---------------------------|------------------------------------------|
| `DATABRICKS_WORKSPACE_URL`| Databricks workspace base URL            |
| `DATABRICKS_TOKEN`        | Personal access token                    |
| `DATABRICKS_WAREHOUSE_ID` | Warehouse ID for executing SQL           |
| `PORT`                    | (Optional) Server port, default `8080`   |
| `LOG_LEVEL`               | (Optional) Logging level, default `DEBUG`|

---

## 🆔 ID Format Summary

| Type     | Format                                |
|----------|----------------------------------------|
| Catalog  | `catalog::<catalog>`                   |
| Schema   | `schema::<catalog>.<schema>`           |
| Table    | `table::<catalog>.<schema>.<table>`    |
| SQL      | `query::<SQL statement>`               |

---

## 🧩 Architecture

```
FastMCP
│
├── search(query) → metadata or SQL stub
└── fetch(id)     → SQL result or metadata
```

---

## ⚠️ ChatGPT Limitations

- ChatGPT **cannot currently execute live SQL** or access external APIs.
- This MCP is intended as a **proof-of-concept** and backend logic must be hosted separately.
- Useful for simulating integrations and planning future assistant capabilities.

---

## 📦 Installation & Run

```bash
pip install fastmcp requests
python server.py
```

Then access via:

```
http://localhost:8080/sse
```

---

## 🧪 Development Setup

### Create and Activate a Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run with MCP Inspector (Optional)

You can test the MCP interface using the Model Context Protocol Inspector:

```bash
npx @modelcontextprotocol/inspector@latest
```
