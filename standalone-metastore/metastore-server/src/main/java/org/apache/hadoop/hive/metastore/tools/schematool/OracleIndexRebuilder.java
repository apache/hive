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
 * Oracle implementation of {@link IndexRebuilder}.
 *
 * <p>Uses {@code USER_INDEXES} / {@code USER_IND_COLUMNS}, automatically scoped to the
 * current user. Rebuilds via {@code ALTER INDEX name REBUILD} — the index is never dropped,
 * so PK constraints and FK references remain intact throughout.
 */
class OracleIndexRebuilder extends AbstractIndexRebuilder {

  // CASE WHEN required: Oracle does not support boolean expressions in SELECT.
  // INDEX_TYPE = 'NORMAL': limits to standard B-tree indexes.
  private static final String QUERY_INDEXES = """
      SELECT INDEX_NAME,
             TABLE_NAME,
             CASE WHEN UNIQUENESS = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique
        FROM USER_INDEXES
       WHERE INDEX_TYPE = 'NORMAL'
       ORDER BY TABLE_NAME, INDEX_NAME""";

  private static final String QUERY_INDEX_COLUMNS = """
      SELECT COLUMN_NAME
        FROM USER_IND_COLUMNS
       WHERE INDEX_NAME = ?
       ORDER BY COLUMN_POSITION""";

  OracleIndexRebuilder(Connection conn, boolean needsQuotedIdentifier, String quoteCharacter) {
    super(conn, needsQuotedIdentifier, quoteCharacter);
  }

  @Override
  public List<IndexInfo> loadIndexes() throws HiveMetaException {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(QUERY_INDEXES)) {
      List<IndexInfo> indexes = new ArrayList<>();
      while (rs.next()) {
        String indexName = rs.getString("INDEX_NAME");
        String tableName = rs.getString("TABLE_NAME");
        boolean isUnique = rs.getInt("is_unique") == 1;
        List<String> columns = loadIndexColumns(indexName);
        // constraintBacked is not meaningful for Oracle as ALTER INDEX REBUILD works
        // identically for all index types
        indexes.add(new IndexInfo(indexName, tableName, isUnique, false, columns));
      }
      return indexes;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to load indexes from Oracle catalog", e);
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
    return MetastoreSchemaTool.quote(
        "ALTER INDEX <qa>" + index.indexName() + "<qa> REBUILD",
        needsQuotedIdentifier, quoteCharacter);
  }

  private List<String> loadIndexColumns(String indexName) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(QUERY_INDEX_COLUMNS)) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME"));
        }
      }
    }
    return columns;
  }
}
