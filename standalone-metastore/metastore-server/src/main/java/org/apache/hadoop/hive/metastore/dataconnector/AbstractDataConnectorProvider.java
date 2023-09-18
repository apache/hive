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

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractDataConnectorProvider implements IDataConnectorProvider {
  protected String scoped_db = null;
  protected Object  handle = null;
  protected DataConnector connector = null;
  protected String driverClassName = null;

  public AbstractDataConnectorProvider(String dbName, DataConnector connector, String driverClassName) {
    this.scoped_db = dbName;
    this.connector = connector;
    this.driverClassName = driverClassName;
  }

  @Override public final void setScope(String scoped_db) {
    if (scoped_db != null)
      this.scoped_db = scoped_db;
  }

  /**
   * Opens a transport/connection to the datasource. Throws exception if the connection cannot be established.
   * @throws MetaException Throws MetaException if the connector does not have all info for a connection to be setup.
   * @throws ConnectException if the connection could not be established for some reason.
   */
  @Override public void open() throws ConnectException, MetaException {

  }

  /**
   * Closes a transport/connection to the datasource.
   * @throws ConnectException if the connection could not be closed.
   */
  @Override public void close() throws ConnectException {

  }

  /**
   * Returns Hive Table objects from the remote database for tables that match a name pattern.
   * @return List A collection of objects that match the name pattern, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param regex
   */
  @Override public List<Table> getTables(String regex) throws MetaException {
    return null;
  }

  /**
   * Returns a list of all table names from the remote database.
   * @return List A collection of all the table names, null if there are no tables.
   * @throws MetaException To indicate any failures with executing this API
   */
  @Override public List<String> getTableNames() throws MetaException {
    return null;
  }

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param tableName
   */
  @Override public Table getTable(String tableName) throws MetaException {
    return null;
  }

  @Override
  public boolean createTable(Table table) throws MetaException {
    throw new MetaException("Creation of table in remote datasource is not supported.");
  }

  @Override
  public boolean dropTable(String tableName) throws MetaException {
    throw new MetaException("Deletion of table in remote datasource is not supported.");
  }

  @Override
  public boolean alterTable(String tableName, Table table) throws MetaException {
    throw new MetaException("Alter table in remote datasource is not supported.");
  }

  protected Table buildTableFromColsList(String tableName, List<FieldSchema> cols) {
    //Setting the storage descriptor.
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setName(tableName);
    serdeInfo.setSerializationLib("org.apache.hive.storage.jdbc.JdbcSerDe");
    Map<String, String> serdeParams = new HashMap<String, String>();
    serdeParams.put("serialization.format", "1");
    serdeInfo.setParameters(serdeParams);

    sd.setSerdeInfo(serdeInfo);
    sd.setInputFormat(getInputClass());
    sd.setOutputFormat(getOutputClass());
    sd.setLocation(getTableLocation(tableName));
    sd.setBucketCols(new ArrayList<String>());
    sd.setSortCols(new ArrayList<Order>());

    //Setting the required table information
    Table table = new Table();
    table.setTableName(tableName);
    table.setTableType(TableType.EXTERNAL_TABLE.toString());
    table.setSd(sd);
    // set table properties that subclasses can fill-in
    table.setParameters(new HashMap<String, String>());
    // set partition keys to empty
    table.setPartitionKeys(new
        ArrayList<FieldSchema>());

    return table;
  }

  abstract protected String getInputClass();

  abstract protected String getOutputClass();

  abstract protected String getTableLocation(String tblName);

  abstract protected String getDatasourceType();

}
