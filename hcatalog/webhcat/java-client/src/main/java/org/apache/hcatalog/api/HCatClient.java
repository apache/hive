/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.api;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema;

/**
 * The abstract class HCatClient containing APIs for HCatalog DDL commands.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.api.HCatClient} instead
 */
public abstract class HCatClient {

  public enum DropDBMode {RESTRICT, CASCADE}

  public static final String HCAT_CLIENT_IMPL_CLASS = "hcat.client.impl.class";

  /**
   * Creates an instance of HCatClient.
   *
   * @param conf An instance of configuration.
   * @return An instance of HCatClient.
   * @throws HCatException
   */
  public static HCatClient create(Configuration conf) throws HCatException {
    HCatClient client = null;
    String className = conf.get(HCAT_CLIENT_IMPL_CLASS,
      HCatClientHMSImpl.class.getName());
    try {
      Class<? extends HCatClient> clientClass = Class.forName(className,
        true, JavaUtils.getClassLoader()).asSubclass(
          HCatClient.class);
      client = (HCatClient) clientClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new HCatException(
        "ClassNotFoundException while creating client class.", e);
    } catch (InstantiationException e) {
      throw new HCatException(
        "InstantiationException while creating client class.", e);
    } catch (IllegalAccessException e) {
      throw new HCatException(
        "IllegalAccessException while creating client class.", e);
    }
    if (client != null) {
      client.initialize(conf);
    }
    return client;
  }

  abstract void initialize(Configuration conf) throws HCatException;

  /**
   * Get all existing databases that match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param pattern  java re pattern
   * @return list of database names
   * @throws HCatException
   */
  public abstract List<String> listDatabaseNamesByPattern(String pattern)
    throws HCatException;

  /**
   * Gets the database.
   *
   * @param dbName The name of the database.
   * @return An instance of HCatDatabaseInfo.
   * @throws HCatException
   */
  public abstract HCatDatabase getDatabase(String dbName) throws HCatException;

  /**
   * Creates the database.
   *
   * @param dbInfo An instance of HCatCreateDBDesc.
   * @throws HCatException
   */
  public abstract void createDatabase(HCatCreateDBDesc dbInfo)
    throws HCatException;

  /**
   * Drops a database.
   *
   * @param dbName The name of the database to delete.
   * @param ifExists Hive returns an error if the database specified does not exist,
   *                 unless ifExists is set to true.
   * @param mode This is set to either "restrict" or "cascade". Restrict will
   *             remove the schema if all the tables are empty. Cascade removes
   *             everything including data and definitions.
   * @throws HCatException
   */
  public abstract void dropDatabase(String dbName, boolean ifExists,
                    DropDBMode mode) throws HCatException;

  /**
   * Returns all existing tables from the specified database which match the given
   * pattern. The matching occurs as per Java regular expressions.
   * @param dbName The name of the DB (to be searched)
   * @param tablePattern The regex for the table-name
   * @return list of table names
   * @throws HCatException
   */
  public abstract List<String> listTableNamesByPattern(String dbName, String tablePattern)
    throws HCatException;

  /**
   * Gets the table.
   *
   * @param dbName The name of the database.
   * @param tableName The name of the table.
   * @return An instance of HCatTableInfo.
   * @throws HCatException
   */
  public abstract HCatTable getTable(String dbName, String tableName)
    throws HCatException;

  /**
   * Creates the table.
   *
   * @param createTableDesc An instance of HCatCreateTableDesc class.
   * @throws HCatException
   */
  public abstract void createTable(HCatCreateTableDesc createTableDesc) throws HCatException;

  /**
   * Updates the Table's column schema to the specified definition.
   *
   * @param dbName The name of the database.
   * @param tableName The name of the table.
   * @param columnSchema The (new) definition of the column schema (i.e. list of fields).
   *
   */
  public abstract void updateTableSchema(String dbName, String tableName, List<HCatFieldSchema> columnSchema)
    throws HCatException;

  /**
   * Creates the table like an existing table.
   *
   * @param dbName The name of the database.
   * @param existingTblName The name of the existing table.
   * @param newTableName The name of the new table.
   * @param ifNotExists If true, then error related to already table existing is skipped.
   * @param isExternal Set to "true", if table has be created at a different
   *                   location other than default.
   * @param location The location for the table.
   * @throws HCatException
   */
  public abstract void createTableLike(String dbName, String existingTblName,
                     String newTableName, boolean ifNotExists, boolean isExternal,
                     String location) throws HCatException;

  /**
   * Drop table.
   *
   * @param dbName The name of the database.
   * @param tableName The name of the table.
   * @param ifExists Hive returns an error if the database specified does not exist,
   *                 unless ifExists is set to true.
   * @throws HCatException
   */
  public abstract void dropTable(String dbName, String tableName,
                   boolean ifExists) throws HCatException;

  /**
   * Renames a table.
   *
   * @param dbName The name of the database.
   * @param oldName The name of the table to be renamed.
   * @param newName The new name of the table.
   * @throws HCatException
   */
  public abstract void renameTable(String dbName, String oldName,
                   String newName) throws HCatException;

  /**
   * Gets all the partitions.
   *
   * @param dbName The name of the database.
   * @param tblName The name of the table.
   * @return A list of partitions.
   * @throws HCatException
   */
  public abstract List<HCatPartition> getPartitions(String dbName, String tblName)
    throws HCatException;

  /**
   * Gets all the partitions that match the specified (and possibly partial) partition specification.
   * A partial partition-specification is one where not all partition-keys have associated values. For example,
   * for a table ('myDb.myTable') with 2 partition keys (dt string, region string),
   * if for each dt ('20120101', '20120102', etc.) there can exist 3 regions ('us', 'uk', 'in'), then,
   *  1. Complete partition spec: getPartitions('myDb', 'myTable', {dt='20120101', region='us'}) would return 1 partition.
   *  2. Partial  partition spec: getPartitions('myDb', 'myTable', {dt='20120101'}) would return all 3 partitions,
   *                              with dt='20120101' (i.e. region = 'us', 'uk' and 'in').
   * @param dbName The name of the database.
   * @param tblName The name of the table.
   * @param partitionSpec The partition specification. (Need not include all partition keys.)
   * @return A list of partitions.
   * @throws HCatException
   */
  public abstract List<HCatPartition> getPartitions(String dbName, String tblName, Map<String, String> partitionSpec)
    throws HCatException;

  /**
   * Gets the partition.
   *
   * @param dbName The database name.
   * @param tableName The table name.
   * @param partitionSpec The partition specification, {[col_name,value],[col_name2,value2]}. All partition-key-values
   *                      must be specified.
   * @return An instance of HCatPartitionInfo.
   * @throws HCatException
   */
  public abstract HCatPartition getPartition(String dbName, String tableName,
                         Map<String, String> partitionSpec) throws HCatException;

  /**
   * Adds the partition.
   *
   * @param partInfo An instance of HCatAddPartitionDesc.
   * @throws HCatException
   */
  public abstract void addPartition(HCatAddPartitionDesc partInfo)
    throws HCatException;

  /**
   * Adds a list of partitions.
   *
   * @param partInfoList A list of HCatAddPartitionDesc.
   * @return The number of partitions added.
   * @throws HCatException
   */
  public abstract int addPartitions(List<HCatAddPartitionDesc> partInfoList)
    throws HCatException;

  /**
   * Drops partition(s) that match the specified (and possibly partial) partition specification.
   * A partial partition-specification is one where not all partition-keys have associated values. For example,
   * for a table ('myDb.myTable') with 2 partition keys (dt string, region string),
   * if for each dt ('20120101', '20120102', etc.) there can exist 3 regions ('us', 'uk', 'in'), then,
   *  1. Complete partition spec: dropPartitions('myDb', 'myTable', {dt='20120101', region='us'}) would drop 1 partition.
   *  2. Partial  partition spec: dropPartitions('myDb', 'myTable', {dt='20120101'}) would drop all 3 partitions,
   *                              with dt='20120101' (i.e. region = 'us', 'uk' and 'in').
   * @param dbName The database name.
   * @param tableName The table name.
   * @param partitionSpec The partition specification, {[col_name,value],[col_name2,value2]}.
   * @param ifExists Hive returns an error if the partition specified does not exist, unless ifExists is set to true.
   * @throws HCatException,ConnectionFailureException
   */
  public abstract void dropPartitions(String dbName, String tableName,
                    Map<String, String> partitionSpec, boolean ifExists)
    throws HCatException;

  /**
   * List partitions by filter.
   *
   * @param dbName The database name.
   * @param tblName The table name.
   * @param filter The filter string,
   *    for example "part1 = \"p1_abc\" and part2 <= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @return list of partitions
   * @throws HCatException
   */
  public abstract List<HCatPartition> listPartitionsByFilter(String dbName, String tblName,
                                 String filter) throws HCatException;

  /**
   * Mark partition for event.
   *
   * @param dbName The database name.
   * @param tblName The table name.
   * @param partKVs the key-values associated with the partition.
   * @param eventType the event type
   * @throws HCatException
   */
  public abstract void markPartitionForEvent(String dbName, String tblName,
                         Map<String, String> partKVs, PartitionEventType eventType)
    throws HCatException;

  /**
   * Checks if a partition is marked for event.
   *
   * @param dbName the db name
   * @param tblName the table name
   * @param partKVs the key-values associated with the partition.
   * @param eventType the event type
   * @return true, if is partition marked for event
   * @throws HCatException
   */
  public abstract boolean isPartitionMarkedForEvent(String dbName, String tblName,
                            Map<String, String> partKVs, PartitionEventType eventType)
    throws HCatException;

  /**
   * Gets the delegation token.
   *
   * @param owner the owner
   * @param renewerKerberosPrincipalName the renewer kerberos principal name
   * @return the delegation token
   * @throws HCatException,ConnectionFailureException
   */
  public abstract String getDelegationToken(String owner,
                        String renewerKerberosPrincipalName) throws HCatException;

  /**
   * Renew delegation token.
   *
   * @param tokenStrForm the token string
   * @return the new expiration time
   * @throws HCatException
   */
  public abstract long renewDelegationToken(String tokenStrForm)
    throws HCatException;

  /**
   * Cancel delegation token.
   *
   * @param tokenStrForm the token string
   * @throws HCatException
   */
  public abstract void cancelDelegationToken(String tokenStrForm)
    throws HCatException;

  /**
   * Retrieve Message-bus topic for a table.
   *
   * @param dbName The name of the DB.
   * @param tableName The name of the table.
   * @return Topic-name for the message-bus on which messages will be sent for the specified table.
   * By default, this is set to <db-name>.<table-name>. Returns null when not set.
   */
  public abstract String getMessageBusTopicName(String dbName, String tableName) throws HCatException;

  /**
   * Close the hcatalog client.
   *
   * @throws HCatException
   */
  public abstract void close() throws HCatException;
}
