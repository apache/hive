# Metastore MCP Server

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes
Hive Metastore metadata via the Iceberg REST Catalog API.

LLM agents can use the provided tools to discover databases, tables, columns,
partitions, and table properties.

## Prerequisites

- Python 3.10+
- A running Hive Metastore with the Iceberg REST Catalog endpoint enabled

### Metastore Configuration

Enable the REST Catalog endpoint in `metastore-site.xml`:

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

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### SSE transport (default) — for Docker / network access

```bash
python3 metastore_mcp_server.py --port 3000 --metastore-url http://localhost:9001/iceberg
```

The server listens on `http://0.0.0.0:3000/sse` and accepts MCP client connections over SSE.

### stdio transport — for local subprocess usage

```bash
python3 metastore_mcp_server.py --transport stdio --metastore-url http://localhost:9001/iceberg
```

Communicates over stdin/stdout. Used when an MCP client spawns the server as a subprocess.

### Docker

When running Hive via Docker Compose, the MCP server runs automatically as a sidecar
process inside the metastore container. Set `MCP_SERVER_ENABLED=true` in the metastore
service environment (already configured in the provided `docker-compose.yml`).

The server is accessible at `http://localhost:3000/sse` from the host.

### Integration with Claude Desktop / Claude Code

If the MCP server is running via Docker (SSE), add to your MCP configuration:

```json
{
  "mcpServers": {
    "metastore": {
      "url": "http://localhost:3000/sse"
    }
  }
}
```

Alternatively, use stdio transport (spawns the server as a subprocess):

```json
{
  "mcpServers": {
    "metastore": {
      "command": "python3",
      "args": ["/path/to/metastore_mcp_server.py", "--transport", "stdio"],
      "env": {
        "METASTORE_REST_URL": "http://localhost:9001/iceberg"
      }
    }
  }
}
```

### Python MCP Client Example

```python
from mcp import ClientSession
from mcp.client.sse import sse_client

async with sse_client("http://localhost:3000/sse") as (read, write):
    async with ClientSession(read, write) as session:
        await session.initialize()

        # List available tools
        tools = await session.list_tools()

        # Get schema for all tables in a database
        result = await session.call_tool(
            'get_table_schema_sql',
            arguments={'database': 'default'}
        )
```

## Available Tools

| Tool | Description |
|------|-------------|
| `list_databases` | List all databases (namespaces) in the metastore |
| `list_tables` | List all tables in a database |
| `describe_table` | Get detailed schema, partitions, and properties for a table |
| `get_table_schema_sql` | Get a SQL-friendly schema summary of all tables in a database |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `METASTORE_REST_URL` | `http://localhost:9001/iceberg` | Metastore Iceberg REST Catalog URL |
| `MCP_SERVER_PORT` | `3000` | Port for SSE transport |
| `MCP_SERVER_ENABLED` | `false` | Set to `true` in Docker to start the sidecar |
