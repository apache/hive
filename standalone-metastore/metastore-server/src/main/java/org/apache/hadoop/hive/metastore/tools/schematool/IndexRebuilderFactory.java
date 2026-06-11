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

import org.apache.hadoop.hive.metastore.HiveMetaException;

/**
 * Factory for creating {@link IndexRebuilder} instances for the given database type.
 * Add support for a new backend by adding a {@code case} branch.
 */
public final class IndexRebuilderFactory {

  private IndexRebuilderFactory() {
  }

  public static IndexRebuilder create(String dbType, Connection conn,
      MetastoreSchemaTool schemaTool) throws HiveMetaException {
    return switch (dbType.toLowerCase()) {
      case HiveSchemaHelper.DB_POSTGRES ->
          new PostgresIndexRebuilder(conn, schemaTool.needsQuotedIdentifier, schemaTool.quoteCharacter);
      case HiveSchemaHelper.DB_MYSQL ->
          new MySQLIndexRebuilder(conn, schemaTool.needsQuotedIdentifier, schemaTool.quoteCharacter);
      case HiveSchemaHelper.DB_ORACLE ->
          new OracleIndexRebuilder(conn, schemaTool.needsQuotedIdentifier, schemaTool.quoteCharacter);
      case HiveSchemaHelper.DB_MSSQL ->
          new MSSQLIndexRebuilder(conn, schemaTool.needsQuotedIdentifier, schemaTool.quoteCharacter);
      default -> throw new HiveMetaException(
          "-rebuildIndexes is not supported for -dbType " + dbType + ".");
    };
  }
}
