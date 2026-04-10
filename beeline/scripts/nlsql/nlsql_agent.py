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
Natural Language to SQL agent for Apache Hive Beeline.

Connects to the Metastore MCP Server (via SSE or stdio) to discover schema,
then uses LangChain + Claude to generate HiveQL.
Prints only the generated SQL to stdout.
"""

import argparse
import asyncio
import os
import sys
import re

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage


async def _call_schema_tool(session, database):
    """Call get_table_schema_sql on an initialized MCP session."""
    result = await session.call_tool(
        'get_table_schema_sql',
        arguments={'database': database}
    )
    schema_text = ''
    for content in result.content:
        if hasattr(content, 'text'):
            schema_text += content.text
    return schema_text


async def get_schema_via_sse(mcp_server_url, database):
    """Connect to a running MCP server via SSE and get schema."""
    async with sse_client(mcp_server_url) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await _call_schema_tool(session, database)


async def get_schema_via_stdio(mcp_server_script, database):
    """Spawn the Metastore MCP server as a subprocess and get schema."""
    metastore_url = os.environ.get('METASTORE_REST_URL', 'http://localhost:9001/iceberg')

    server_params = StdioServerParameters(
        command='python3',
        args=[mcp_server_script, '--transport', 'stdio'],
        env={**os.environ, 'METASTORE_REST_URL': metastore_url},
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await _call_schema_tool(session, database)


def generate_sql(schema_info, nl_query, database):
    """Use LangChain + Claude to convert natural language to HiveQL."""
    base_url = os.environ.get('ANTHROPIC_BASE_URL', 'https://api.anthropic.com')
    api_key = os.environ.get('ANTHROPIC_AUTH_TOKEN',
                             os.environ.get('ANTHROPIC_API_KEY', ''))
    model = os.environ.get('ANTHROPIC_MODEL', 'claude-sonnet-4-20250514')

    llm = ChatAnthropic(
        model=model,
        anthropic_api_url=base_url,
        anthropic_api_key=api_key,
        max_tokens=1024,
        temperature=0,
    )

    system_prompt = f"""You are a HiveQL expert. Convert the user's natural language request into a valid HiveQL query.

RULES:
- Output ONLY the SQL query, nothing else. No markdown, no explanation, no code fences.
- Use HiveQL syntax (not MySQL or PostgreSQL).
- The current database is `{database}`.
- Use ONLY the tables and columns listed in the schema below. Do NOT reference tables that don't exist.
- If the request cannot be answered with the available schema, write the closest possible query using the actual tables.
- Always include a LIMIT clause for SELECT queries unless the user explicitly asks for all rows.

SCHEMA:
{schema_info}"""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=nl_query),
    ]

    response = llm.invoke(messages)
    sql = response.content.strip()
    # Strip markdown code fences if the model wraps the output
    sql = re.sub(r'^```(?:sql)?\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip()


async def async_main(args):
    mcp_server_url = os.environ.get('MCP_SERVER_URL', '')

    if mcp_server_url:
        # Connect to remote MCP server via SSE
        try:
            schema_info = await get_schema_via_sse(mcp_server_url, args.database)
        except Exception as e:
            print(f'Warning: MCP SSE connection failed: {e}', file=sys.stderr)
            schema_info = '(Schema not available)'
    else:
        # Fall back to spawning MCP server as subprocess
        # In the source tree: beeline/scripts/nlsql/ -> standalone-metastore/metastore-tools/mcp-server/
        script_dir = os.path.dirname(os.path.abspath(__file__))
        source_root = os.path.join(script_dir, '..', '..', '..')
        candidates = [
            os.path.join(source_root, 'standalone-metastore', 'metastore-tools',
                         'mcp-server', 'metastore_mcp_server.py'),
        ]
        mcp_server_script = None
        for candidate in candidates:
            candidate = os.path.normpath(candidate)
            if os.path.exists(candidate):
                mcp_server_script = candidate
                break

        if mcp_server_script is None:
            print('Warning: MCP server not found in any known location',
                  file=sys.stderr)
            schema_info = '(Schema not available - MCP server not found)'
        else:
            try:
                schema_info = await get_schema_via_stdio(mcp_server_script, args.database)
            except Exception as e:
                print(f'Warning: MCP schema discovery failed: {e}', file=sys.stderr)
                schema_info = '(Schema not available)'

    # Generate SQL
    try:
        sql = generate_sql(schema_info, args.query, args.database)
    except Exception as e:
        print(f'Error generating SQL: {e}', file=sys.stderr)
        sys.exit(1)

    # Print ONLY the SQL to stdout
    print(sql)


def main():
    parser = argparse.ArgumentParser(description='Natural Language to HiveQL')
    parser.add_argument('--query', required=True,
                        help='Natural language query')
    parser.add_argument('--database', default='default',
                        help='Current database name')
    parser.add_argument('--metastore-url',
                        default=os.environ.get('METASTORE_REST_URL',
                                               'http://localhost:9001/iceberg'),
                        help='Metastore Iceberg REST Catalog URL (stdio fallback only)')
    args = parser.parse_args()

    # Set env var so the MCP server picks it up (stdio fallback)
    os.environ['METASTORE_REST_URL'] = args.metastore_url.rstrip('/')

    asyncio.run(async_main(args))


if __name__ == '__main__':
    main()
