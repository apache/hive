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
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared duplicate-check and DDL execution logic for {@link IndexRebuilder}. */
public abstract class AbstractIndexRebuilder implements IndexRebuilder {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexRebuilder.class);

  protected final Connection conn;
  protected final boolean needsQuotedIdentifier;
  protected final String quoteCharacter;

  protected AbstractIndexRebuilder(Connection conn, boolean needsQuotedIdentifier,
      String quoteCharacter) {
    this.conn = conn;
    this.needsQuotedIdentifier = needsQuotedIdentifier;
    this.quoteCharacter = quoteCharacter;
  }

  @Override
  public long findDuplicates(IndexInfo index) throws HiveMetaException {
    if (!index.unique()) {
      return 0;
    }
    String quotedCols = index.columns().stream()
        .map(c -> "<qa>" + c + "<qa>")
        .collect(Collectors.joining(", "));
    String sql = MetastoreSchemaTool.quote(
        "SELECT COUNT(*) FROM (SELECT " + quotedCols
            + " FROM <qa>" + index.tableName() + "<qa>"
            + " GROUP BY " + quotedCols
            + " HAVING COUNT(*) > 1) duplicates",
        needsQuotedIdentifier, quoteCharacter);
    try (PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0;
    } catch (SQLException e) {
      throw new HiveMetaException(
          "Failed to check for duplicate rows for index \"" + index.indexName() + "\"", e);
    }
  }

  /** Executes one or more DDL statements, logging each before execution. */
  protected void executeRebuild(IndexInfo index, String... ddls) throws HiveMetaException {
    try (Statement stmt = conn.createStatement()) {
      for (String ddl : ddls) {
        LOG.info("Executing: {}", ddl);
        stmt.execute(ddl);
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to rebuild index \"" + index.indexName() + "\"", e);
    }
  }
}
