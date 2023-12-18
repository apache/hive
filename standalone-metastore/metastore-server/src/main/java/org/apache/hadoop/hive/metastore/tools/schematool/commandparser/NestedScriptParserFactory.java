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
package org.apache.hadoop.hive.metastore.tools.schematool.commandparser;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.DB_DERBY;
import static org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.DB_HIVE;
import static org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.DB_MSSQL;
import static org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.DB_MYSQL;
import static org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.DB_ORACLE;
import static org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.DB_POSTGRACE;

/**
 * Factpry class for creating and returning the proper {@link NestedScriptParser} implementations for the given
 * database type. for list of supported database types see: {@link org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper}
 */
public class NestedScriptParserFactory {

  /**
   * This methods <b>must not be used by production code</b> as it is returning a non-valid configuration.
   * It is only here for testing purposes.
   */
  @VisibleForTesting
  public NestedScriptParser getNestedScriptParser(String dbName, String dbOpts) {
    if (dbName.equalsIgnoreCase(DB_DERBY)) {
      return new DerbyCommandParser(dbOpts, false);
    } else if (dbName.equalsIgnoreCase(DB_MSSQL)) {
      return new MSSQLCommandParser(dbOpts, false);
    } else if (dbName.equalsIgnoreCase(DB_MYSQL)) {
      return new MySqlCommandParser(dbOpts, false);
    } else if (dbName.equalsIgnoreCase(DB_POSTGRACE)) {
      return new PostgresCommandParser(dbOpts, false);
    } else if (dbName.equalsIgnoreCase(DB_ORACLE)) {
      return new OracleCommandParser(dbOpts, false);
    } else {
      throw new IllegalArgumentException("Unknown dbType " + dbName);
    }
  }

  public NestedScriptParser getNestedScriptParser(String dbName, String dbOpts, String metaDbName) {
    if (dbName.equalsIgnoreCase(DB_DERBY)) {
      return new DerbyCommandParser(dbOpts, true);
    } else if (dbName.equalsIgnoreCase(DB_HIVE)) {
      return new HiveCommandParser(dbOpts, false,
          getNestedScriptParser(metaDbName, null, null).getQuoteCharacter());
    } else if (dbName.equalsIgnoreCase(DB_MSSQL)) {
      return new MSSQLCommandParser(dbOpts, true);
    } else if (dbName.equalsIgnoreCase(DB_MYSQL)) {
      return new MySqlCommandParser(dbOpts, true);
    } else if (dbName.equalsIgnoreCase(DB_POSTGRACE)) {
      return new PostgresCommandParser(dbOpts, true);
    } else if (dbName.equalsIgnoreCase(DB_ORACLE)) {
      return new OracleCommandParser(dbOpts, true);
    } else {
      throw new IllegalArgumentException("Unknown dbType " + dbName);
    }
  }

}