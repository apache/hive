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
package org.apache.hive.hcatalog.api;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

/**
 * The abstract class HCatClient containing APIs for HCatalog DDL commands.
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
        true, Utilities.getSessionSpecifiedClassLoader()).asSubclass(
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
   * Fetch configuration value on conf that the HCatClient is instantiated
   * against. We do not want to expose the conf itself via a getConf(), because
   * we do not want it modifiable after instantiation of the HCatClient, but
   * modules that get called from HCatClient often need to know about how
   * HCatClient is configured, so we want a read-only interface for it.
   *
   * @param key keyname to look up
   * @param defaultVal default value to furnish in case the key does not exist
   * @return value for given key, and defaultVal if key is not present in conf
   */
  public abstract String getConfVal(String key, String defaultVal);

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
   * Updates the Table's whole schema (including column schema, I/O Formats, SerDe definitions, etc.)
   * @param dbName The name of the database.
   * @param tableName The name of the table.
   * @param newTableDefinition The (new) definition of the table.
   * @throws HCatException
   */
  public abstract void updateTableSchema(String dbName, String tableName, HCatTable newTableDefinition)
      throws HCatException;

  /**
   * Serializer for HCatTable.
   * @param hcatTable The HCatTable to be serialized into string form
   * @return String representation of the HCatTable.
   * @throws HCatException, on failure to serialize.
   */
  public abstract String serializeTable(HCatTable hcatTable) throws HCatException;

  /**
   * Deserializer for HCatTable.
   * @param hcatTableStringRep The String representation of an HCatTable, presumably retrieved from {@link #serializeTable(HCatTable)}
   * @return HCatTable reconstructed from the string
   * throws HCatException, on failure to deserialize.
   */
  public abstract HCatTable deserializeTable(String hcatTableStringRep) throws HCatException;

  /**
   * Serializer for HCatPartition.
   * @param hcatPartition The HCatPartition instance to be serialized.
   * @return String representation of the HCatPartition.
   * @throws HCatException, on failure to serialize.
   */
  public abstract String serializePartition(HCatPartition hcatPartition) throws HCatException;

  /**
   * Serializer for a list of HCatPartition.
   * @param hcatPartitions The HCatPartitions to be serialized.
   * @return A list of Strings, each representing an HCatPartition.
   * @throws HCatException, on failure to serialize.
   */
  public abstract List<String> serializePartitions(List<HCatPartition> hcatPartitions) throws HCatException;

  /**
   * Deserializer for an HCatPartition.
   * @param hcatPartitionStringRep The String representation of the HCatPartition, presumably retrieved from {@link #serializePartition(HCatPartition)}
   * @return HCatPartition instance reconstructed from the string.
   * @throws HCatException, on failure to deserialze.
   */
  public abstract HCatPartition deserializePartition(String hcatPartitionStringRep) throws HCatException;

  /**
   * Deserializer for a list of HCatPartition strings.
   * @param hcatPartitionStringReps The list of HCatPartition strings to be deserialized.
   * @return A list of HCatPartition instances, each reconstructed from an entry in the string-list.
   * @throws HCatException, on failure to deserialize.
   */
  public abstract List<HCatPartition> deserializePartitions(List<String> hcatPartitionStringReps) throws HCatException;

  /**
   * Serializer for HCatPartitionSpec.
   * @param partitionSpec HCatPartitionSpec to be serialized.
   * @return A list of Strings, representing the HCatPartitionSpec as a whole.
   * @throws HCatException On failure to serialize.
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract List<String> serializePartitionSpec(HCatPartitionSpec partitionSpec) throws HCatException;

  /**
   * Deserializer for HCatPartitionSpec.
   * @param hcatPartitionSpecStrings List of strings, representing the HCatPartitionSpec as a whole.
   * @return HCatPartitionSpec, reconstructed from the list of strings.
   * @throws HCatException On failure to deserialize.
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract HCatPartitionSpec deserializePartitionSpec(List<String> hcatPartitionSpecStrings) throws HCatException;

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
   * Gets partitions in terms of generic HCatPartitionSpec instances.
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract HCatPartitionSpec getPartitionSpecs(String dbName, String tableName, int maxPartitions) throws HCatException;

  /**
   * Gets partitions in terms of generic HCatPartitionSpec instances.
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract HCatPartitionSpec getPartitionSpecs(String dbName, String tableName, Map<String, String> partitionSelector, int maxPartitions)
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
   * Adds partitions using HCatPartitionSpec.
   * @param partitionSpec The HCatPartitionSpec representing the set of partitions added.
   * @return The number of partitions added.
   * @throws HCatException On failure to add partitions.
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract int addPartitionSpec(HCatPartitionSpec partitionSpec)
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
   * @param deleteData Whether to delete the underlying data.
   * @throws HCatException,ConnectionFailureException
   */
   public abstract void dropPartitions(String dbName, String tableName,
                    Map<String, String> partitionSpec, boolean ifExists, boolean deleteData)
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
   * List partitions by filter, but as HCatPartitionSpecs.
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract HCatPartitionSpec listPartitionSpecsByFilter(String dbName, String tblName,
                                                               String filter, int maxPartitions) throws HCatException;

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
   * Get an iterator that iterates over a list of replication tasks needed to replicate all the
   * events that have taken place for a given db/table.
   * @param lastEventId : The last event id that was processed for this reader. The returned
   *                    replication tasks will start from this point forward
   * @param maxEvents : Maximum number of events to consider for generating the
   *                  replication tasks. If < 1, then all available events will be considered.
   * @param dbName : The database name for which we're interested in the events for.
   * @param tableName : The table name for which we're interested in the events for - if null,
   *                  then this function will behave as if it were running at a db level.
   * @return an iterator over a list of replication events that can be processed one by one.
   * @throws HCatException
   */
  @InterfaceStability.Evolving
  public abstract Iterator<ReplicationTask> getReplicationTasks(
      long lastEventId, int maxEvents, String dbName, String tableName) throws HCatException;

  /**
   * Get a list of notifications
   * @param lastEventId The last event id that was consumed by this reader.  The returned
   *                    notifications will start at the next eventId available this eventId that
   *                    matches the filter.
   * @param maxEvents Maximum number of events to return.  If < 1, then all available events will
   *                  be returned.
   * @param filter Filter to determine if message should be accepted.  If null, then all
   *               available events up to maxEvents will be returned.
   * @return list of notifications, sorted by eventId.  It is guaranteed that the events are in
   * the order that the operations were done on the database.
   * @throws HCatException
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract List<HCatNotificationEvent> getNextNotification(long lastEventId,
                                                                  int maxEvents,
                                                                  IMetaStoreClient.NotificationFilter filter)
      throws HCatException;

  /**
   * Get the most recently used notification id.
   * @return
   * @throws HCatException
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract long getCurrentNotificationEventId() throws HCatException;

  /**
   * Close the hcatalog client.
   *
   * @throws HCatException
   */
  public abstract void close() throws HCatException;
}
