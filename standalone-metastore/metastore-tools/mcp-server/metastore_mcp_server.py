#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Metastore MCP Server — exposes Hive Metastore metadata via Model Context Protocol.

Backed by the Metastore Iceberg REST Catalog API. Provides tools for LLM agents
to discover databases, tables, columns, partitions, and table properties.

Usage:
    python3 metastore_mcp_server.py [--metastore-url http://localhost:9001/iceberg]

    # Or via env var:
    METASTORE_REST_URL=http://localhost:9001/iceberg python3 metastore_mcp_server.py
"""

import argparse
import os
import json

import requests
from mcp.server.fastmcp import FastMCP

# Create MCP server
mcp = FastMCP("metastore-mcp", instructions="""
You have access to a Hive Metastore via the Iceberg REST Catalog API.
Use the tools below to discover databases, tables, and their schemas
before writing HiveQL queries. Always check available tables first.
""")

METASTORE_URL = os.environ.get("METASTORE_REST_URL", "http://localhost:9001/iceberg").rstrip("/")


def _load_table_metadata(database, table_name):
    """Fetch table metadata from the REST Catalog API."""
    resp = requests.get(
        f"{METASTORE_URL}/v1/namespaces/{database}/tables/{table_name}", timeout=10)
    resp.raise_for_status()
    return resp.json().get("metadata", {})


def _get_current_schema(metadata):
    """Extract the current schema from Iceberg table metadata."""
    current_schema_id = metadata.get("current-schema-id", 0)
    schemas = metadata.get("schemas", [])
    return next((s for s in schemas if s.get("schema-id") == current_schema_id),
                schemas[0] if schemas else {})


def _parse_columns(schema):
    """Parse column definitions from an Iceberg schema."""
    columns = []
    for field in schema.get("fields", []):
        col_type = field.get("type", "unknown")
        if isinstance(col_type, dict):
            col_type = col_type.get("type", "complex")
        columns.append({
            "name": field.get("name"),
            "type": col_type,
            "required": field.get("required", False),
        })
    return columns


def _get_partition_fields(metadata):
    """Extract partition fields from the default partition spec."""
    partition_specs = metadata.get("partition-specs", [])
    if not partition_specs:
        return []
    default_spec_id = metadata.get("default-spec-id", 0)
    spec = next((s for s in partition_specs if s.get("spec-id") == default_spec_id), None)
    return spec.get("fields", []) if spec else []


@mcp.tool()
def list_databases() -> str:
    """List all databases (namespaces) in the Hive Metastore."""
    resp = requests.get(f"{METASTORE_URL}/v1/namespaces", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    namespaces = [ns[0] if isinstance(ns, list) else ns
                  for ns in data.get("namespaces", [])]
    return json.dumps(namespaces, indent=2)


@mcp.tool()
def list_tables(database: str = "default") -> str:
    """List all tables in a database.

    Args:
        database: The database/namespace name (default: "default")
    """
    resp = requests.get(f"{METASTORE_URL}/v1/namespaces/{database}/tables", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    tables = [ident["name"] for ident in data.get("identifiers", [])]
    return json.dumps(tables, indent=2)


@mcp.tool()
def describe_table(table: str, database: str = "default") -> str:
    """Get detailed schema information for a table including columns,
    types, partition spec, and key properties.

    Args:
        table: The table name
        database: The database/namespace name (default: "default")
    """
    metadata = _load_table_metadata(database, table)
    schema = _get_current_schema(metadata)

    props = metadata.get("properties", {})
    interesting = ["format-version", "write.format.default", "comment",
                   "engine.hive.enabled", "numRows", "totalSize"]

    result = {
        "table": f"{database}.{table}",
        "columns": _parse_columns(schema),
        "partition_fields": _get_partition_fields(metadata),
        "properties": {k: v for k, v in props.items() if k in interesting},
        "location": metadata.get("location", ""),
        "format_version": metadata.get("format-version", ""),
    }
    return json.dumps(result, indent=2)


@mcp.tool()
def get_table_schema_sql(database: str = "default") -> str:
    """Get a SQL-friendly schema summary of ALL tables in a database.
    Useful as context before generating SQL queries.

    Args:
        database: The database/namespace name (default: "default")
    """
    # List tables
    resp = requests.get(f"{METASTORE_URL}/v1/namespaces/{database}/tables", timeout=10)
    resp.raise_for_status()
    tables = [ident["name"] for ident in resp.json().get("identifiers", [])]

    schema_parts = []
    for table_name in tables:
        try:
            metadata = _load_table_metadata(database, table_name)
            schema = _get_current_schema(metadata)
            columns = _parse_columns(schema)

            col_defs = [f"  {col['name']} {col['type']}" for col in columns]

            part_info = ""
            part_fields = _get_partition_fields(metadata)
            if part_fields:
                pnames = [f.get("name", "") for f in part_fields]
                part_info = f"\n  -- PARTITIONED BY: {', '.join(pnames)}"

            schema_parts.append(
                f"TABLE {database}.{table_name} (\n" +
                ",\n".join(col_defs) +
                part_info +
                "\n)")
        except Exception:
            pass

    return "\n\n".join(schema_parts) if schema_parts else "(No tables found)"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Metastore MCP Server")
    parser.add_argument("--metastore-url",
                        default=os.environ.get("METASTORE_REST_URL",
                                               "http://localhost:9001/iceberg"),
                        help="Metastore REST Catalog URL")
    parser.add_argument("--transport", default="sse",
                        choices=["sse", "stdio"],
                        help="MCP transport (default: sse)")
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("MCP_SERVER_PORT", "3000")),
                        help="Port for SSE transport (default: 3000)")
    args = parser.parse_args()
    METASTORE_URL = args.metastore_url.rstrip("/")

    if args.transport == "sse":
        mcp.settings.host = "0.0.0.0"
        mcp.settings.port = args.port
    mcp.run(transport=args.transport)
