# Natural Language to SQL (nlsql) for Apache Hive Beeline

Converts natural language queries to HiveQL using an LLM, then executes them automatically.

## Prerequisites

- Python 3.10+
- A running Hive Metastore with the Iceberg REST Catalog endpoint enabled
- An Anthropic API key or compatible LLM gateway

## Installation

Install Python dependencies for both the nlsql agent and the Metastore MCP server:

```bash
pip install -r $HIVE_HOME/scripts/nlsql/requirements.txt
pip install -r $HIVE_HOME/scripts/metastore/mcp-server/requirements.txt
```

## Configuration

Set the following environment variables before starting Beeline:

```bash
export ANTHROPIC_BASE_URL="https://api.anthropic.com"   # or your gateway URL
export ANTHROPIC_AUTH_TOKEN="your-token"                 # or use ANTHROPIC_API_KEY
export ANTHROPIC_MODEL="claude-sonnet-4-20250514"        # optional, this is the default
export METASTORE_REST_URL="http://localhost:9001/iceberg" # optional, this is the default
```

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_BASE_URL` | `https://api.anthropic.com` | API base URL (use this for proxies or gateways) |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` | Model to use for SQL generation |
| `ANTHROPIC_AUTH_TOKEN` | _(none)_ | Auth token for the LLM API |
| `ANTHROPIC_API_KEY` | _(none)_ | Fallback if `ANTHROPIC_AUTH_TOKEN` is not set |
| `METASTORE_REST_URL` | `http://localhost:9001/iceberg` | Metastore Iceberg REST Catalog URL |

### Metastore REST Catalog

The Metastore must have the Iceberg REST Catalog endpoint enabled. Add to `metastore-site.xml`:

```xml
<property>
  <name>metastore.catalog.servlet.port</name>
  <value>9001</value>
</property>
<property>
  <name>metastore.catalog.servlet.auth</name>
  <value>none</value>
</property>
```

## Usage

Connect to HiveServer2 via Beeline, then use the `!nlsql` command:

```
$ beeline -u jdbc:hive2://localhost:10000

beeline> !nlsql show me the top 10 orders by amount
beeline> !nlsql count all rows in the customers table
beeline> !nlsql which tables have more than 1 million rows
```

The agent will:
1. Discover the schema of the current database via the Metastore MCP server
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
nlsql_agent.py -- spawns MCP server subprocess, gets schema, calls LLM
  |
  v
metastore_mcp_server.py -- queries HMS Iceberg REST Catalog API
```
