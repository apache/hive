# Natural Language to SQL (nlsql) for Apache Hive Beeline

Converts natural language queries to HiveQL using an LLM, then executes them automatically.

## Prerequisites

- Python 3.10+
- An Anthropic API key or compatible LLM gateway
- A running Hive Metastore (Docker Compose or standalone)

## Installation

Install the nlsql Python dependencies:

```bash
pip install -r $HIVE_HOME/scripts/nlsql/requirements.txt
```

For the stdio fallback (non-Docker), also install the MCP server dependencies — see
[Local (stdio)](#local-stdio--fallback) below.

## Configuration

Set the following environment variables before starting Beeline:

```bash
export ANTHROPIC_BASE_URL="https://api.anthropic.com"   # or your gateway URL
export ANTHROPIC_AUTH_TOKEN="your-token"                 # or use ANTHROPIC_API_KEY
export ANTHROPIC_MODEL="claude-sonnet-4-20250514"        # optional, this is the default
```

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_BASE_URL` | `https://api.anthropic.com` | API base URL (use this for proxies or gateways) |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` | Model to use for SQL generation |
| `ANTHROPIC_AUTH_TOKEN` | _(none)_ | Auth token for the LLM API |
| `ANTHROPIC_API_KEY` | _(none)_ | Fallback if `ANTHROPIC_AUTH_TOKEN` is not set |

## MCP Server Connection

The nlsql agent connects to the Metastore MCP Server to discover schema. There are two modes:

### Docker (SSE) — recommended

When running Hive via Docker Compose, the MCP server runs as a sidecar process inside
the metastore container, exposed on port 3000. Set `MCP_SERVER_URL` to connect:

```bash
export MCP_SERVER_URL="http://localhost:3000/sse"
```

No additional setup is needed — the Docker image includes the MCP server and its
Python dependencies.

### Local (stdio) — fallback

If `MCP_SERVER_URL` is not set, the agent falls back to spawning the MCP server as
a local subprocess via stdio. This requires the MCP server script available locally
and a running Metastore with the Iceberg REST Catalog endpoint enabled.

The agent searches for `metastore_mcp_server.py` in the source tree at
`standalone-metastore/metastore-tools/mcp-server/` (relative to the repo root).

Install the MCP server Python dependencies:
```bash
pip install -r path/to/mcp-server/requirements.txt
```

Configure the metastore REST catalog URL:
```bash
export METASTORE_REST_URL="http://localhost:9001/iceberg"
```

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_SERVER_URL` | _(none)_ | MCP server SSE endpoint (e.g. `http://localhost:3000/sse`) |
| `METASTORE_REST_URL` | `http://localhost:9001/iceberg` | HMS REST Catalog URL (only used in stdio fallback) |

## Usage

Connect to HiveServer2 via Beeline, then use the `!nlsql` command:

```
$ beeline -u jdbc:hive2://localhost:10000

beeline> !nlsql show me the top 10 orders by amount
beeline> !nlsql count all rows in the customers table
beeline> !nlsql which tables have more than 1 million rows
```

The agent will:
1. Discover the schema of the current database via the Metastore MCP Server
2. Send the schema and your natural language query to the LLM
3. Display the generated SQL
4. Execute it against HiveServer2

## How It Works

```
!nlsql <natural language query>
  |
  v
Beeline (Java) -- spawns Python subprocess
  |
  v
nlsql_agent.py
  |-- MCP_SERVER_URL set? --> connects via SSE (http://host:3000/sse)
  |-- not set?            --> spawns metastore_mcp_server.py via stdio
  |
  v
metastore_mcp_server.py -- queries HMS Iceberg REST Catalog API
```
