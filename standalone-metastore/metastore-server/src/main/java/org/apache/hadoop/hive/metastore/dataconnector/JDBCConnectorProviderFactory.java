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

package org.apache.hadoop.hive.metastore.dataconnector;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.DerbySQLConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.HiveJDBCConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.MySQLConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.PostgreSQLConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.OracleConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.MSSQLConnectorProvider;

import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.*;

public class JDBCConnectorProviderFactory {

  public static IDataConnectorProvider get(String dbName, DataConnector connector) {
    IDataConnectorProvider provider = null;
    switch(connector.getType().toLowerCase()) {
    case MYSQL_TYPE:
      provider = new MySQLConnectorProvider(dbName, connector);
      break;
    case POSTGRES_TYPE:
      provider = new PostgreSQLConnectorProvider(dbName, connector);
      break;

    case DERBY_TYPE:
      provider = new DerbySQLConnectorProvider(dbName, connector);
      break;

    case ORACLE_TYPE:
      provider = new OracleConnectorProvider(dbName, connector);
      break;

    case MSSQL_TYPE:
      provider = new MSSQLConnectorProvider(dbName, connector);
      break;

    case HIVE_JDBC_TYPE:
      provider = new HiveJDBCConnectorProvider(dbName, connector);
      break;

    default:
      throw new RuntimeException("Unsupported JDBC type");
    }

    return provider;
  }
}
