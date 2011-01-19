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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;

/**
 * TODO Unnecessary when the server sides for both dbstore and filestore are
 * merged
 */
public interface IMetaStoreClient {

  public void close();

  /**
   * Get the names of all databases in the MetaStore that match the given pattern.
   * @param databasePattern
   * @return List of database names.
   * @throws MetaException
   * @throws TException
   */
  public List<String> getDatabases(String databasePattern)
      throws MetaException, TException;

  /**
   * Get the names of all databases in the MetaStore.
   * @return List of database names.
   * @throws MetaException
   * @throws TException
   */
  public List<String> getAllDatabases()
      throws MetaException, TException;

  /**
   * Get the names of all tables in the specified database that satisfy the supplied
   * table name pattern.
   * @param dbName
   * @param tablePattern
   * @return List of table names.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   */
  public List<String> getTables(String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get the names of all tables in the specified database.
   * @param dbName
   * @return List of table names.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   */
  public List<String> getAllTables(String dbName)
      throws MetaException, TException, UnknownDBException;


  /**
   * Drop the table.
   *
   * @param dbname
   *          The database for this table
   * @param tableName
   *          The table to drop
   * @throws MetaException
   *           Could not drop table properly.
   * @throws NoSuchObjectException
   *           The table wasn't found.
   * @throws TException
   *           A thrift communication error occurred
   * @throws ExistingDependentsException
   */
  public void dropTable(String dbname, String tableName, boolean deleteData,
      boolean ignoreUknownTab) throws MetaException, TException,
      NoSuchObjectException;

  /**
   * Drop the table in the DEFAULT database.
   *
   * @param tableName
   *          The table to drop
   * @param deleteData
   *          Should we delete the underlying data
   * @throws MetaException
   *           Could not drop table properly.
   * @throws UnknownTableException
   *           The table wasn't found.
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           The table wasn't found.
   *
   * @deprecated As of release 0.6.0 replaced by {@link #dropTable(String, String, boolean, boolean)}.
   *             This method will be removed in release 0.7.0.
   */
  @Deprecated
  public void dropTable(String tableName, boolean deleteData)
      throws MetaException, UnknownTableException, TException,
      NoSuchObjectException;

  public void dropTable(String dbname, String tableName)
      throws MetaException, TException, NoSuchObjectException;

  public boolean tableExists(String databaseName, String tableName) throws MetaException,
      TException, UnknownDBException;

  /**
   * Check to see if the specified table exists in the DEFAULT database.
   * @param tableName
   * @return TRUE if DEFAULT.tableName exists, FALSE otherwise.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   * @deprecated As of release 0.6.0 replaced by {@link #tableExists(String, String)}.
   *             This method will be removed in release 0.7.0.
   */
  @Deprecated
  public boolean tableExists(String tableName) throws MetaException,
      TException, UnknownDBException;

  /**
   * Get a table object from the DEFAULT database.
   *
   * @param tableName
   *          Name of the table to fetch.
   * @return An object representing the table.
   * @throws MetaException
   *           Could not fetch the table
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           In case the table wasn't found.
   * @deprecated As of release 0.6.0 replaced by {@link #getTable(String, String)}.
   *             This method will be removed in release 0.7.0.
   */
  @Deprecated
  public Table getTable(String tableName) throws MetaException, TException,
      NoSuchObjectException;

  /**
   * Get a Database Object
   * @param databaseName  name of the database to fetch
   * @return
   * @throws NoSuchObjectException The database does not exist
   * @throws MetaException Could not fetch the database
   * @throws TException A thrift communication error occurred
   */
    public Database getDatabase(String databaseName)
        throws NoSuchObjectException, MetaException, TException;


  /**
   * Get a table object.
   *
   * @param dbName
   *          The database the table is located in.
   * @param tableName
   *          Name of the table to fetch.
   * @return An object representing the table.
   * @throws MetaException
   *           Could not fetch the table
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           In case the table wasn't found.
   */
  public Table getTable(String dbName, String tableName) throws MetaException,
      TException, NoSuchObjectException;

  /**
   * @param tableName
   * @param dbName
   * @param partVals
   * @return the partition object
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  public Partition appendPartition(String tableName, String dbName,
      List<String> partVals) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException;

  public Partition appendPartition(String tableName, String dbName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;
  /**
   * Add a partition to the table.
   *
   * @param partition
   *          The partition to add
   * @return The partition added
   * @throws InvalidObjectException
   *           Could not find table to add to
   * @throws AlreadyExistsException
   *           Partition already exists
   * @throws MetaException
   *           Could not add partition
   * @throws TException
   *           Thrift exception
   */
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException;

  /**
   * @param tblName
   * @param dbName
   * @param partVals
   * @return the partition object
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  public Partition getPartition(String tblName, String dbName,
      List<String> partVals) throws NoSuchObjectException, MetaException, TException;

  /**
   * @param dbName
   * @param tblName
   * @param name - partition name i.e. 'ds=2010-02-03/ts=2010-02-03 18%3A16%3A01'
   * @return the partition object
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  public Partition getPartition(String dbName, String tblName,
      String name) throws MetaException, UnknownTableException, NoSuchObjectException, TException;

  
  /**
   * @param dbName
   * @param tableName
   * @param pvals
   * @param userName
   * @param groupNames
   * @return
   * @throws MetaException
   * @throws UnknownTableException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public Partition getPartitionWithAuthInfo(String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException;
  
  /**
   * @param tbl_name
   * @param db_name
   * @param max_parts
   * @return the list of partitions
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  public List<Partition> listPartitions(String db_name, String tbl_name,
      short max_parts) throws NoSuchObjectException, MetaException, TException;

  public List<Partition> listPartitions(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws NoSuchObjectException, MetaException, TException;

  public List<String> listPartitionNames(String db_name, String tbl_name,
      short max_parts) throws MetaException, TException;

  public List<String> listPartitionNames(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws MetaException, TException;

  /**
   * @param dbName
   * @param tableName
   * @param s
   * @param userName
   * @param groupNames
   * @return
   * @throws NoSuchObjectException 
   */
  public List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, short s, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * @param dbName
   * @param tableName
   * @param partialPvals
   * @param s
   * @param userName
   * @param groupNames
   * @return
   * @throws NoSuchObjectException 
   */
  public List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, List<String> partialPvals, short s, String userName,
      List<String> groupNames) throws MetaException, TException, NoSuchObjectException;

  /**
   * @param tbl
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  public void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException;

  public void alter_table(String defaultDatabaseName, String tblName,
      Table table) throws InvalidOperationException, MetaException, TException;

  public void createDatabase(Database db)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  public void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  public void alterDatabase(String name, Database db)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * @param db_name
   * @param tbl_name
   * @param part_vals
   * @param deleteData
   *          delete the underlying data or just delete the table in metadata
   * @return true or false
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
   *      java.lang.String, java.util.List, boolean)
   */
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;

  public boolean dropPartition(String db_name, String tbl_name,
      String name, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;
  /**
   * updates a partition to new partition
   *
   * @param dbName
   *          database of the old partition
   * @param tblName
   *          table name of the old partition
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the old partition does not exist
   * @throws MetaException
   *           if error in updating metadata
   * @throws TException
   *           if error in communicating with metastore server
   */
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException;

  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_fields(java.lang.String,
   *      java.lang.String)
   */
  public List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_schema(java.lang.String,
   *      java.lang.String)
   */
  public List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  /**
   * @param name
   *          name of the configuration property to get the value of
   * @param defaultValue
   *          the value to return if property with the given name doesn't exist
   * @return value of the specified configuration property
   * @throws TException
   * @throws ConfigValSecurityException
   */
  public String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException;

  /**
   *
   * @param name
   *          the partition name e.g. ("ds=2010-03-03/hr=12")
   * @return a list containing the partition col values, in the same order as the name
   * @throws MetaException
   * @throws TException
   */
  public List<String> partitionNameToVals(String name)
      throws MetaException, TException;
  /**
   *
   * @param name
   *          the partition name e.g. ("ds=2010-03-03/hr=12")
   * @return a map from the partition col to the value, as listed in the name
   * @throws MetaException
   * @throws TException
   */
  public Map<String, String> partitionNameToSpec(String name)
      throws MetaException, TException;

  /**
   * create an index
   * @param index the index object
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @throws AlreadyExistsException
   */
  public void createIndex(Index index, Table indexTable) throws InvalidObjectException,
      MetaException, NoSuchObjectException, TException, AlreadyExistsException;

  public void alter_index(String dbName, String tblName, String indexName,
      Index index) throws InvalidOperationException, MetaException, TException;

  /**
   *
   * @param dbName
   * @param tblName
   * @param indexName
   * @return
   * @throws MetaException
   * @throws UnknownTableException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public Index getIndex(String dbName, String tblName, String indexName)
      throws MetaException, UnknownTableException, NoSuchObjectException,
      TException;


  /**
   * list indexes of the give base table
   * @param db_name
   * @param tbl_name
   * @param max
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  public List<Index> listIndexes(String db_name, String tbl_name,
      short max) throws NoSuchObjectException, MetaException, TException;

  /**
   * list all the index names of the give base table.
   *
   * @param db_name
   * @param tbl_name
   * @param max
   * @return
   * @throws MetaException
   * @throws TException
   */
  public List<String> listIndexNames(String db_name, String tbl_name,
      short max) throws MetaException, TException;

  /**
   * @param db_name
   * @param tbl_name
   * @param name index name
   * @param deleteData
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  public boolean dropIndex(String db_name, String tbl_name,
      String name, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;
  
  /**
   * @param Role
   *          role object
   * @return
   * @throws MetaException
   * @throws TException
   */
  public boolean create_role(Role role)
      throws MetaException, TException;

  /**
   * @param role_name
   *          role name
   * @param db_name 
   * 
   * @return
   * @throws MetaException
   * @throws TException
   */
  public boolean drop_role(String role_name) throws MetaException, TException;

  /**
   * list all role names
   * @return
   * @throws TException 
   * @throws MetaException 
   */
  public List<String> listRoleNames() throws MetaException, TException;

  /**
   * 
   * @param role_name
   * @param user_name
   * @param principalType
   * @param grantor
   * @param grantorType
   * @param grantOption
   * @return
   * @throws MetaException
   * @throws TException
   */
  public boolean grant_role(String role_name, String user_name,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, TException;

  /**
   * @param role_name
   *          role name
   * @param user_name
   *          user name
   * @param principalType
   * @param db_name
   * 
   * @return
   * @throws MetaException
   * @throws TException
   */
  public boolean revoke_role(String role_name, String user_name,
      PrincipalType principalType) throws MetaException, TException;

  /**
   * 
   * @param principalName
   * @param principalType
   * @return
   * @throws MetaException
   * @throws TException
   */
  public List<Role> list_roles(String principalName, PrincipalType principalType)
      throws MetaException, TException;

  /**
   * @param hiveObject
   * @param user_name
   * @param group_names
   * @return
   * @throws MetaException
   * @throws TException
   */
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String user_name, List<String> group_names) throws MetaException,
      TException;
  
  /**
   * @param principal_name
   * @param principal_type
   * @param hiveObject
   * @return
   * @throws MetaException
   * @throws TException
   */
  public List<HiveObjectPrivilege> list_privileges(String principal_name,
      PrincipalType principal_type, HiveObjectRef hiveObject)
      throws MetaException, TException;

  /**
   * @param privileges
   * @return
   * @throws MetaException
   * @throws TException
   */
  public boolean grant_privileges(PrivilegeBag privileges)
      throws MetaException, TException;

  /**
   * @param privileges
   * @return
   * @throws MetaException
   * @throws TException
   */
  public boolean revoke_privileges(PrivilegeBag privileges)
      throws MetaException, TException;

  /**
   * @param renewerKerberosPrincipalName
   * @param tokenSignature
   * @return
   * @throws MetaException
   * @throws TException
   */
  public String getDelegationTokenWithSignature(String renewerKerberosPrincipalName, String tokenSignature)
      throws MetaException, TException;

  /**
   * @param renewerKerberosPrincipalName
   * @return
   * @throws MetaException
   * @throws TException
   */
  public String getDelegationToken(String renewerKerberosPrincipalName)
      throws MetaException, TException;

  /**
   * @param tokenStrForm
   * @return
   * @throws MetaException
   * @throws TException
   */
  public long renewDelegationToken(String tokenStrForm) throws MetaException, TException;

  /**
   * @param tokenStrForm
   * @throws MetaException
   * @throws TException
   */
  public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException;


}
