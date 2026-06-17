/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.tools.schematool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaException;

/**
 * Postgres implementation of {@link IndexRebuilder}.
 *
 * <p>Uses catalog-sourced DDL via {@code pg_get_indexdef} / {@code pg_get_constraintdef}.
 * Constraint-backed indexes use DROP/ADD CONSTRAINT; standalone indexes use DROP/CREATE INDEX.
 */
class PostgresIndexRebuilder extends AbstractIndexRebuilder {

  // ic = index class, tc = table class. Restrict to btree and use pg_constraint to
  // distinguish constraint-backed indexes from standalone indexes.
  // relkind: 'i' = index, 'r' = ordinary table (excludes views, partitions, etc.).
  private static final String QUERY_INDEXES = """
    SELECT ic.relname AS indexname, tc.relname AS tablename, ix.indisunique,
           (con.conname IS NOT NULL) AS constraint_backed,  -- true when index is owned by a table constraint
           pg_get_indexdef(ic.oid)       AS index_def,      -- catalog-generated CREATE INDEX statement
           con.conname                   AS constraint_name,
           pg_get_constraintdef(con.oid) AS constraint_def  -- catalog-generated constraint definition fragment
      FROM pg_index ix
      JOIN pg_class ic ON ic.oid = ix.indexrelid AND ic.relkind = 'i'
      JOIN pg_class tc ON tc.oid = ix.indrelid   AND tc.relkind = 'r'
      JOIN pg_am am    ON am.oid = ic.relam      AND am.amname = 'btree'  -- restrict to btree indexes only
 LEFT JOIN pg_constraint con ON con.conindid = ic.oid  -- links index to PK/UNIQUE constraint when present
     WHERE ic.relnamespace = current_schema()::regnamespace  -- only objects in the active schema
    """;

private static final String QUERY_INDEX_COLUMNS = """
    SELECT a.attname
      FROM pg_index ix
      JOIN pg_class ic    ON ic.oid = ix.indexrelid AND ic.relkind = 'i'
      JOIN pg_attribute a ON a.attrelid = ix.indrelid  -- read columns from the base table of the index
                         AND a.attnum = ANY(ix.indkey) -- keep attrs whose attnum appears in index key vector
                         AND a.attnum > 0  -- system columns have negative attnum; exclude them
     WHERE ic.relname = ? AND ic.relnamespace = current_schema()::regnamespace  -- scope name lookup to active schema
     ORDER BY array_position(ix.indkey, a.attnum)  -- ix.indkey stores attr numbers in key order
    """;

  private record PgDdl(String dropDdl, String createDdl) {}

  private final Map<String, PgDdl> ddlMap = new LinkedHashMap<>();

  PostgresIndexRebuilder(Connection conn, boolean needsQuotedIdentifier, String quoteCharacter) {
    super(conn, needsQuotedIdentifier, quoteCharacter);
  }

  @Override
  public List<IndexInfo> loadIndexes() throws HiveMetaException {
    ddlMap.clear();
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(QUERY_INDEXES)) {
      List<IndexInfo> indexes = new ArrayList<>();
      while (rs.next()) {
        String indexName = rs.getString("indexname");
        String tableName = rs.getString("tablename");
        boolean isUnique = rs.getBoolean("indisunique");
        boolean isConstraintBacked = rs.getBoolean("constraint_backed");
        String indexDef = rs.getString("index_def");
        String constraintName = rs.getString("constraint_name");
        String constraintDef = rs.getString("constraint_def");
        List<String> columns = loadIndexColumns(indexName);

        String dropDdl;
        String createDdl;
        if (isConstraintBacked) {
          dropDdl = MetastoreSchemaTool.quote(
              "ALTER TABLE ONLY <qa>" + tableName + "<qa>"
              + " DROP CONSTRAINT <qa>" + constraintName + "<qa>",
              needsQuotedIdentifier, quoteCharacter);
          createDdl = MetastoreSchemaTool.quote(
              "ALTER TABLE ONLY <qa>" + tableName + "<qa>"
              + " ADD CONSTRAINT <qa>" + constraintName + "<qa> " + constraintDef,
              needsQuotedIdentifier, quoteCharacter);
        } else {
          dropDdl = MetastoreSchemaTool.quote(
              "DROP INDEX IF EXISTS <qa>" + indexName + "<qa>",
              needsQuotedIdentifier, quoteCharacter);
          createDdl = indexDef;
        }
        ddlMap.put(indexName, new PgDdl(dropDdl, createDdl));
        indexes.add(new IndexInfo(indexName, tableName, isUnique, isConstraintBacked, columns));
      }
      return indexes;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to load indexes from Postgres catalog", e);
    }
  }

  @Override
  public void rebuildIndex(IndexInfo index) throws HiveMetaException {
    PgDdl ddl = ddlMap.get(index.indexName());
    boolean prevAutoCommit;
    try {
      prevAutoCommit = conn.getAutoCommit();
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get autocommit state", e);
    }
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      executeRebuild(index, ddl.dropDdl(), ddl.createDdl());
      conn.commit();
      success = true;
    } catch (SQLException e) {
      throw new HiveMetaException("Transaction error rebuilding index \"" + index.indexName() + "\"", e);
    } finally {
      if (!success) {
        try {
          conn.rollback(); 
        } catch (SQLException ignored) {}
      }
      try {
        conn.setAutoCommit(prevAutoCommit);
      } catch (SQLException ignored) {}
    }
  }

  @Override
  public String describeRebuildDDL(IndexInfo index) {
    PgDdl ddl = ddlMap.get(index.indexName());
    return ddl.dropDdl() + ";" + System.lineSeparator() + ddl.createDdl() + ";";
  }

  private List<String> loadIndexColumns(String indexName) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(QUERY_INDEX_COLUMNS)) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString("attname"));
        }
      }
    }
    return columns;
  }
}
