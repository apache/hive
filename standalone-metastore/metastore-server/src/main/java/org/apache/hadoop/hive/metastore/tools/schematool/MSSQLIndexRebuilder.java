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
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaException;

/**
 * SQL Server implementation of {@link IndexRebuilder}.
 *
 * <p>Uses {@code sys.indexes} / {@code sys.index_columns} / {@code sys.columns} and rebuilds
 * with {@code ALTER INDEX name ON table REBUILD}.
 */
class MSSQLIndexRebuilder extends AbstractIndexRebuilder {
  
  // i.type: 1 = clustered, 2 = nonclustered.
  // i.name IS NOT NULL excludes heap pseudo-entries (type 0).
  private static final String QUERY_INDEXES = """
      SELECT i.name  AS index_name,
             t.name  AS table_name,
             i.is_unique
        FROM sys.indexes i
        JOIN sys.tables t ON t.object_id = i.object_id
       WHERE t.is_ms_shipped = 0
         AND i.type IN (1, 2)
         AND i.name IS NOT NULL
       ORDER BY t.name, i.name""";

  // INCLUDE columns are not key columns; exclude them.
  private static final String QUERY_INDEX_COLUMNS = """
      SELECT c.name AS column_name
        FROM sys.indexes i
        JOIN sys.tables t          ON t.object_id = i.object_id
        JOIN sys.index_columns ic  ON ic.object_id = i.object_id
                                  AND ic.index_id  = i.index_id
        JOIN sys.columns c         ON c.object_id  = ic.object_id
                                  AND c.column_id  = ic.column_id
       WHERE t.name = ? AND i.name = ?
         AND ic.is_included_column = 0
       ORDER BY ic.key_ordinal""";

  MSSQLIndexRebuilder(Connection conn, boolean needsQuotedIdentifier, String quoteCharacter) {
    super(conn, needsQuotedIdentifier, quoteCharacter);
  }

  @Override
  public List<IndexInfo> loadIndexes() throws HiveMetaException {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(QUERY_INDEXES)) {
      List<IndexInfo> indexes = new ArrayList<>();
      while (rs.next()) {
        String indexName = rs.getString("index_name");
        String tableName = rs.getString("table_name");
        boolean isUnique = rs.getBoolean("is_unique");
        List<String> columns = loadIndexColumns(tableName, indexName);
        // constraintBacked is irrelevant for MSSQL: ALTER INDEX REBUILD works identically
        // for all index types
        indexes.add(new IndexInfo(indexName, tableName, isUnique, false, columns));
      }
      return indexes;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to load indexes from SQL Server catalog", e);
    }
  }

  @Override
  public void rebuildIndex(IndexInfo index) throws HiveMetaException {
    executeRebuild(index, buildRebuildDdl(index));
  }

  @Override
  public String describeRebuildDDL(IndexInfo index) {
    return buildRebuildDdl(index) + ";";
  }

  private String buildRebuildDdl(IndexInfo index) {
    // MSSQL requires the table name because index names are unique per-table, not per-schema.
    return MetastoreSchemaTool.quote(
        "ALTER INDEX <qa>" + index.indexName() + "<qa>"
            + " ON <qa>" + index.tableName() + "<qa> REBUILD",
        needsQuotedIdentifier, quoteCharacter);
  }

  private List<String> loadIndexColumns(String tableName, String indexName) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(QUERY_INDEX_COLUMNS)) {
      ps.setString(1, tableName);
      ps.setString(2, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString("column_name"));
        }
      }
    }
    return columns;
  }
}

