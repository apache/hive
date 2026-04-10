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

### Standalone

```bash
# Using command-line argument
python3 metastore_mcp_server.py --metastore-url http://localhost:9001/iceberg

# Using environment variable
METASTORE_REST_URL=http://localhost:9001/iceberg python3 metastore_mcp_server.py
```

The server communicates over stdio using the MCP protocol.

### Integration with Claude Desktop / Claude Code

Add to your MCP configuration:

```json
{
  "mcpServers": {
    "metastore": {
      "command": "python3",
      "args": ["/path/to/metastore_mcp_server.py"],
      "env": {
        "METASTORE_REST_URL": "http://localhost:9001/iceberg"
      }
    }
  }
}
```

### Integration with LangChain

```python
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

server_params = StdioServerParameters(
    command='python3',
    args=['metastore_mcp_server.py'],
    env={'METASTORE_REST_URL': 'http://localhost:9001/iceberg'},
)

async with stdio_client(server_params) as (read, write):
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

### Integration with Any MCP-Compatible Client

The server exposes standard MCP tools over stdio. Any MCP client can connect by
spawning the server as a subprocess and communicating via stdin/stdout.

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
