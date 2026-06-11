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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.HiveMetaException;

/**
 * MySQL/MariaDB implementation of {@link IndexRebuilder}.
 *
 * <p>Loads metadata from {@code INFORMATION_SCHEMA.STATISTICS} and rebuilds with one
 * atomic {@code ALTER TABLE DROP ..., ADD ...} statement.
 */
class MySQLIndexRebuilder extends AbstractIndexRebuilder {

  // Keep key column order by SEQ_IN_INDEX.
  // PRIMARY is the only special drop form (DROP PRIMARY KEY); other UNIQUE definitions are
  // dropped as named indexes.
  private static final String QUERY_INDEXES = """
      SELECT TABLE_NAME,
             INDEX_NAME,
             (NON_UNIQUE = 0)            AS is_unique,
             (INDEX_NAME = 'PRIMARY')    AS constraint_backed,
             COLUMN_NAME
        FROM INFORMATION_SCHEMA.STATISTICS
       WHERE TABLE_SCHEMA = DATABASE()
         AND INDEX_TYPE = 'BTREE'
       ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX""";

  MySQLIndexRebuilder(Connection conn, boolean needsQuotedIdentifier, String quoteCharacter) {
    super(conn, needsQuotedIdentifier, quoteCharacter);
  }

  @Override
  public void rebuildIndex(IndexInfo index) throws HiveMetaException {
    executeRebuild(index, buildAtomicRebuildDdl(index));
  }

  @Override
  public String describeRebuildDDL(IndexInfo index) {
    return buildAtomicRebuildDdl(index) + ";";
  }

  private String buildAtomicRebuildDdl(IndexInfo index) {
    String quotedCols = index.columns().stream()
        .map(c -> "<qa>" + c + "<qa>")
        .collect(Collectors.joining(", "));
    String template;
    if (index.constraintBacked()) {
      // DROP PRIMARY KEY takes no name.
      template = "ALTER TABLE <qa>" + index.tableName() + "<qa>"
          + " DROP PRIMARY KEY, ADD PRIMARY KEY (" + quotedCols + ")";
    } else {
      template = "ALTER TABLE <qa>" + index.tableName() + "<qa>"
          + " DROP INDEX <qa>" + index.indexName() + "<qa>,"
          + (index.unique() ? " ADD UNIQUE INDEX " : " ADD INDEX ")
          + "<qa>" + index.indexName() + "<qa>"
          + " (" + quotedCols + ") USING BTREE";
    }
    return MetastoreSchemaTool.quote(template, needsQuotedIdentifier, quoteCharacter);
  }

  @Override
  public List<IndexInfo> loadIndexes() throws HiveMetaException {
    // STATISTICS returns one row per index column; accumulate rows into one index object.
    LinkedHashMap<String, IndexAccumulator> byKey = new LinkedHashMap<>();
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(QUERY_INDEXES)) {
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        String indexName = rs.getString("INDEX_NAME");
        boolean isUnique = rs.getBoolean("is_unique");
        boolean isConstraintBacked = rs.getBoolean("constraint_backed");
        String column = rs.getString("COLUMN_NAME");
        // Use a delimiter to avoid collisions when table/index names are concatenated.
        byKey.computeIfAbsent(tableName + "\0" + indexName,
            k -> new IndexAccumulator(indexName, tableName, isUnique, isConstraintBacked))
            .columns.add(column);
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to load indexes from MySQL catalog", e);
    }
    List<IndexInfo> indexes = new ArrayList<>(byKey.size());
    for (IndexAccumulator acc : byKey.values()) {
      indexes.add(new IndexInfo(acc.indexName, acc.tableName, acc.unique, acc.constraintBacked,
          acc.columns));
    }
    return indexes;
  }

  private static final class IndexAccumulator {
    final String indexName;
    final String tableName;
    final boolean unique;
    final boolean constraintBacked;
    final List<String> columns = new ArrayList<>();

    IndexAccumulator(String indexName, String tableName, boolean unique, boolean constraintBacked) {
      this.indexName = indexName;
      this.tableName = tableName;
      this.unique = unique;
      this.constraintBacked = constraintBacked;
    }
  }
}
