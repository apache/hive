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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.ConnectException;
import java.util.List;

/**
 * This interface provides a way for us to plugin different datasources into hive.
 * Each implementing class plugs in a new datasource type that the hive metastore can then call on
 * The implementing class understand the hive model objects and provide the translation between hive and the
 * native datasource.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IDataConnectorProvider {
  public static final String MYSQL_TYPE = "mysql";
  public static final String POSTGRES_TYPE = "postgres";
  public static final String ORACLE_TYPE = "oracle";
  public static final String MSSQL_TYPE = "mssql";
  public static final String DERBY_TYPE = "derby";
  public static final String HIVE_JDBC_TYPE = "hivejdbc";

  DataConnector connector = null;

  /**
   * Opens a transport/connection to the datasource. Throws exception if the connection cannot be established.
   * @throws MetaException Throws MetaException if the connector does not have all info for a connection to be setup.
   * @throws java.net.ConnectException if the connection could not be established for some reason.
   */
  public void open() throws ConnectException, MetaException;

  /**
   * Closes a transport/connection to the datasource.
   * @throws ConnectException if the connection could not be closed.
   */
  public void close() throws ConnectException;

  /**
   * Set the scope of this object.
   */
  public void setScope(String databaseName);

  /**
   * Returns Hive Table objects from the remote database for tables that match a name pattern.
   * @return List A collection of objects that match the name pattern, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   */
  List<Table> getTables(String regex) throws MetaException;

  /**
   * Returns a list of all table names from the remote database.
   * @return List A collection of all the table names, null if there are no tables.
   * @throws MetaException To indicate any failures with executing this API
   */
  List<String> getTableNames() throws MetaException;

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   */
  Table getTable(String tableName) throws MetaException;

  /**
   * Creates a table with the given name in the remote data source. Conversion between hive data types
   * and native data types is handled by the provider.
   * @param table A Hive table object to translate and create in remote data source.
   * @return boolean True if the operation succeeded or false otherwise
   * @throws MetaException To indicate any failures in executing this operation.
   */
  boolean createTable(Table table) throws  MetaException;

  /**
   * Drop an existing table with the given name in the remote data source.
   * @param tableName Table name to drop from the remote data source.
   * @return boolean True if the operation succeeded or false otherwise
   * @throws MetaException To indicate any failures in executing this operation.
   */
  boolean dropTable(String tableName) throws MetaException;

  /**
   * Alter an existing table in the remote data source.
   * @param tableName Table name to alter in the remote datasource.
   * @param newTable New Table object to modify the existing table with.
   * @return boolean True if the operation succeeded or false otherwise.
   * @throws MetaException To indicate any failures in executing this operation.
   */
  boolean alterTable(String tableName, Table newTable) throws MetaException;
}
