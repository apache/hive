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

package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLConnectorProvider extends AbstractJDBCConnectorProvider {
  private static Logger LOG = LoggerFactory.getLogger(MySQLConnectorProvider.class);

  private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

  public MySQLConnectorProvider(String dbName, DataConnector dataConn) {
    super(dbName, dataConn, DRIVER_CLASS);
  }

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param tableName
   */
  @Override public ResultSet fetchTableMetadata(String tableName) throws MetaException {
    try {
      Statement stmt = getConnection().createStatement();
      ResultSet rs = stmt.executeQuery(
          "SELECT table_name, column_name, is_nullable, data_type, character_maximum_length FROM INFORMATION_SCHEMA.Columns where table_schema='"
              + scoped_db + "' and table_name='" + tableName + "'");
      return rs;
    } catch (Exception e) {
      LOG.warn("Exception retrieving remote table " + scoped_db + "." + tableName + " via data connector "
          + connector.getName());
      throw new MetaException("Error retrieving remote table:" + e);
    }
  }

  @Override protected String getCatalogName() {
    return scoped_db;
  }

  @Override protected String getDatabaseName() {
    return null;
  }

  protected String getDataType(String dbDataType, int size) {
    String mappedType = super.getDataType(dbDataType, size);
    if (!mappedType.equalsIgnoreCase(ColumnType.VOID_TYPE_NAME)) {
      return mappedType;
    }

    // map any db specific types here.
    switch (dbDataType.toLowerCase())
    {
    default:
      mappedType = ColumnType.VOID_TYPE_NAME;
      break;
    }
    return mappedType;
  }
}
