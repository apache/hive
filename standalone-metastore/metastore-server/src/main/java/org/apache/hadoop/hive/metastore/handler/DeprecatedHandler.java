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

package org.apache.hadoop.hive.metastore.handler;

import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.AppendPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CreateDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionRequest;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.GetDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.GetFieldsRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprResult;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByFilterRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetSchemaRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TruncateTableRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.rethrowException;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * This handler collects all deprecated APIs, those APIs are
 * supposed to be dropped in the future release including the thrift definition.
 */
public abstract class DeprecatedHandler extends BaseHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DeprecatedHandler.class);

  protected DeprecatedHandler(String name, Configuration conf) {
    super(name, conf);
  }

  @Override
  @Deprecated
  public void create_database(final Database db) throws TException {
    CreateDatabaseRequest req = new CreateDatabaseRequest(db.getName());
    req.setDescription(db.getDescription());
    req.setLocationUri(db.getLocationUri());
    req.setParameters(db.getParameters());
    req.setPrivileges(db.getPrivileges());
    req.setOwnerName(db.getOwnerName());
    req.setOwnerType(db.getOwnerType());
    req.setCatalogName(db.getCatalogName());
    req.setCreateTime(db.getCreateTime());
    req.setManagedLocationUri(db.getManagedLocationUri());
    req.setType(db.getType());
    req.setDataConnectorName(db.getConnector_name());
    req.setRemote_dbname(db.getRemote_dbname());

    create_database_req(req);
    //location and manged location might be set/changed.
    db.setLocationUri(req.getLocationUri());
    db.setManagedLocationUri(req.getManagedLocationUri());
  }

  @Override
  @Deprecated
  public Database get_database(final String name) throws TException {
    GetDatabaseRequest request = new GetDatabaseRequest();
    String[] parsedDbName = parseDbName(name, conf);
    request.setName(parsedDbName[DB_NAME]);
    if (parsedDbName[CAT_NAME] != null) {
      request.setCatalogName(parsedDbName[CAT_NAME]);
    }
    return get_database_req(request);
  }

  @Override
  @Deprecated
  public void alter_database(final String dbName, final Database newDB) throws TException {
    AlterDatabaseRequest alterDbReq = new AlterDatabaseRequest(dbName, newDB);
    alter_database_req(alterDbReq);
  }

  @Override
  @Deprecated
  public void drop_database(final String dbName, final boolean deleteData, final boolean cascade)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);

    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(parsedDbName[DB_NAME]);
    req.setCatalogName(parsedDbName[CAT_NAME]);
    req.setDeleteData(deleteData);
    req.setCascade(cascade);
    drop_database_req(req);
  }

  @Override
  @Deprecated
  public void create_table(final Table tbl) throws TException {
    CreateTableRequest createTableReq = new CreateTableRequest(tbl);
    create_table_req(createTableReq);
  }

  @Override
  @Deprecated
  public void create_table_with_environment_context(final Table tbl,
      final EnvironmentContext envContext)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    startFunction("create_table_with_environment_context", ": " + tbl.getTableName());
    boolean success = false;
    Exception ex = null;
    try {
      CreateTableRequest createTableReq = new CreateTableRequest(tbl);
      if (envContext != null) {
        createTableReq.setEnvContext(envContext);
      }
      create_table_req(createTableReq);
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_table_with_environment_context", success, ex, tbl.getTableName());
    }
  }

  @Override
  @Deprecated
  public void create_table_with_constraints(final Table tbl,
      final List<SQLPrimaryKey> primaryKeys, final List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    startFunction("create_table_with_constraints", ": " + tbl.toString());
    boolean success = false;
    Exception ex = null;
    try {
      CreateTableRequest req = new CreateTableRequest(tbl);
      req.setPrimaryKeys(primaryKeys);
      req.setForeignKeys(foreignKeys);
      req.setUniqueConstraints(uniqueConstraints);
      req.setNotNullConstraints(notNullConstraints);
      req.setDefaultConstraints(defaultConstraints);
      req.setCheckConstraints(checkConstraints);
      create_table_req(req);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_table_with_constraints", success, ex, tbl.getTableName());
    }
  }

  @Override
  @Deprecated
  public void drop_table(final String dbname, final String name, final boolean deleteData) throws TException {
    String[] parsedDbName = parseDbName(dbname, conf);
    DropTableRequest dropTableReq = new DropTableRequest(parsedDbName[DB_NAME], name);
    dropTableReq.setDeleteData(deleteData);
    dropTableReq.setCatalogName(parsedDbName[CAT_NAME]);
    dropTableReq.setDropPartitions(true);
    drop_table_req(dropTableReq);
  }

  @Override
  @Deprecated
  public void drop_table_with_environment_context(final String dbname, final String name, final boolean deleteData,
      final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(dbname, conf);
    DropTableRequest dropTableReq = new DropTableRequest(parsedDbName[DB_NAME], name);
    dropTableReq.setDeleteData(deleteData);
    dropTableReq.setCatalogName(parsedDbName[CAT_NAME]);
    dropTableReq.setDropPartitions(true);
    dropTableReq.setEnvContext(envContext);
    drop_table_req(dropTableReq);
  }

  @Deprecated
  @Override
  public void truncate_table(final String dbName, final String tableName, List<String> partNames) throws TException {
    // Deprecated path, won't work for txn tables.
    TruncateTableRequest truncateTableReq = new TruncateTableRequest(dbName, tableName);
    truncateTableReq.setPartNames(partNames);
    truncate_table_req(truncateTableReq);
  }

  @Override
  @Deprecated
  public Partition append_partition(final String dbName, final String tableName,
      final List<String> part_vals) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    AppendPartitionsRequest appendPartitionsReq = new AppendPartitionsRequest();
    appendPartitionsReq.setDbName(parsedDbName[DB_NAME]);
    appendPartitionsReq.setTableName(tableName);
    appendPartitionsReq.setPartVals(part_vals);
    appendPartitionsReq.setCatalogName(parsedDbName[CAT_NAME]);
    return append_partition_req(appendPartitionsReq);
  }

  @Override
  @Deprecated
  public Partition append_partition_with_environment_context(final String dbName,
      final String tableName, final List<String> part_vals, final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    AppendPartitionsRequest appendPartitionsReq = new AppendPartitionsRequest();
    appendPartitionsReq.setDbName(parsedDbName[DB_NAME]);
    appendPartitionsReq.setTableName(tableName);
    appendPartitionsReq.setPartVals(part_vals);
    appendPartitionsReq.setCatalogName(parsedDbName[CAT_NAME]);
    appendPartitionsReq.setEnvironmentContext(envContext);
    return append_partition_req(appendPartitionsReq);
  }

  @Deprecated
  @Override
  public int add_partitions(final List<Partition> parts) throws MetaException,
      InvalidObjectException, AlreadyExistsException {
    if (parts == null) {
      throw new MetaException("Partition list cannot be null.");
    }
    if (parts.isEmpty()) {
      return 0;
    }
    String catName = parts.get(0).isSetCatName() ? parts.get(0).getCatName() : getDefaultCatalog(conf);
    String dbName = parts.get(0).getDbName();
    String tableName = parts.get(0).getTableName();
    startTableFunction("add_partitions", catName, dbName, tableName);

    Integer ret = null;
    Exception ex = null;
    try {
      // Old API assumed all partitions belong to the same table; keep the same assumption
      if (!parts.get(0).isSetCatName()) {
        parts.forEach(p -> p.setCatName(catName));
      }
      AddPartitionsRequest addPartitionsReq = new AddPartitionsRequest();
      addPartitionsReq.setDbName(parts.get(0).getDbName());
      addPartitionsReq.setTblName(parts.get(0).getTableName());
      addPartitionsReq.setParts(parts);
      addPartitionsReq.setIfNotExists(false);
      if (parts.get(0).isSetCatName()) {
        addPartitionsReq.setCatName(parts.get(0).getCatName());
      }
      ret = add_partitions_req(addPartitionsReq).getPartitions().size();
      assert ret == parts.size();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("add_partitions", ret != null, ex, tableName);
    }
    return ret;
  }

  @Deprecated
  @Override
  public Partition add_partition(final Partition part)
      throws InvalidObjectException, AlreadyExistsException, MetaException {
    return add_partition_with_environment_context(part, null);
  }

  @Deprecated
  @Override
  public Partition add_partition_with_environment_context( final Partition part, EnvironmentContext envContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException {
    if (part == null) {
      throw new MetaException("Partition cannot be null.");
    }
    AddPartitionsRequest addPartitionsReq = new AddPartitionsRequest(part.getDbName(), part.getTableName(),
        new ArrayList<>(Arrays.asList(part)), false);
    addPartitionsReq.setEnvironmentContext(envContext);
    try {
      return add_partitions_req(addPartitionsReq).getPartitions().get(0);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class,
          AlreadyExistsException.class).defaultMetaException();
    }
  }

  @Deprecated
  @Override
  public boolean drop_partition(final String db_name, final String tbl_name,
      final List<String> part_vals, final boolean deleteData)
      throws TException {
    return drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData,
        null);
  }


  @Deprecated
  @Override
  public boolean drop_partition_with_environment_context(final String db_name, final String tbl_name, final List<String> part_vals,
      final boolean deleteData, final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    DropPartitionRequest dropPartitionReq = new DropPartitionRequest(parsedDbName[DB_NAME], tbl_name);
    dropPartitionReq.setCatName(parsedDbName[CAT_NAME]);
    dropPartitionReq.setPartVals(part_vals);
    dropPartitionReq.setDeleteData(deleteData);
    dropPartitionReq.setEnvironmentContext(envContext);
    return drop_partition_req(dropPartitionReq);
  }

  /**
   * Use {@link #get_partition_req(GetPartitionRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public Partition get_partition(final String db_name, final String tbl_name,
      final List<String> part_vals) throws MetaException, NoSuchObjectException {
    try {
      String[] parsedDbName = parseDbName(db_name, conf);
      GetPartitionRequest getPartitionRequest = new GetPartitionRequest(parsedDbName[DB_NAME], tbl_name, part_vals);
      getPartitionRequest.setCatName(parsedDbName[CAT_NAME]);
      return get_partition_req(getPartitionRequest).getPartition();
    } catch (TException e) {
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultMetaException();
    }
  }

  @Override
  @Deprecated
  public Partition get_partition_with_auth(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final String user_name, final List<String> group_names)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    String partName = GetPartitionsHandler.validatePartVals(this, tableName, part_vals);
    GetPartitionsPsWithAuthRequest gpar = new GetPartitionsPsWithAuthRequest(tableName.getDb(), tableName.getTable());
    gpar.setCatName(tableName.getCat());
    gpar.setUserName(user_name);
    gpar.setGroupNames(group_names);
    gpar.setPartNames(List.of(partName));
    List<Partition> partitions = GetPartitionsHandler.getPartitions(
        t ->  startFunction("get_partition_with_auth",
            " : tbl=" + t + samplePartitionValues(part_vals) + getGroupsCountAndUsername(user_name,group_names)),
        rex ->   endFunction("get_partition_with_auth",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tbl_name),
        this, tableName, gpar, true);
    return partitions.getFirst();
  }

  /**
   * Use {@link #get_partitions_req(PartitionsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<Partition> get_partitions(final String db_name, final String tbl_name,
      final short max_parts) throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    return GetPartitionsHandler.getPartitions(
        t -> startTableFunction("get_partitions", tableName.getCat(), tableName.getDb(), tableName.getTable()),
        rex -> endFunction("get_partitions",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, GetPartitionsHandler.createPartitionsRequest(tableName, max_parts), false);
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_with_auth(final String dbName,
      final String tblName, final short maxParts, final String userName,
      final List<String> groupNames) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);
    GetPartitionsPsWithAuthRequest getAuthReq = new GetPartitionsPsWithAuthRequest(tableName.getDb(), tableName.getTable());
    getAuthReq.setCatName(tableName.getCat());
    getAuthReq.setUserName(userName);
    getAuthReq.setGroupNames(groupNames);
    getAuthReq.setMaxParts(maxParts);
    return get_partitions_ps_with_auth_req(getAuthReq).getPartitions();
  }

  @Override
  @Deprecated
  public List<PartitionSpec> get_partitions_pspec(final String db_name, final String tbl_name, final int max_parts)
      throws NoSuchObjectException, MetaException  {

    String[] parsedDbName = parseDbName(db_name, conf);
    String catName = parsedDbName[CAT_NAME];
    String dbName = parsedDbName[DB_NAME];
    String tableName = tbl_name.toLowerCase();

    startPartitionFunction("get_partitions_pspec", catName, dbName, tableName, max_parts);

    List<PartitionSpec> partitionSpecs = null;
    try {
      GetTableRequest getTableRequest = new GetTableRequest(dbName, tableName);
      getTableRequest.setCatName(catName);
      Table table = get_table_core(getTableRequest);
      // get_partitions will parse out the catalog and db names itself
      TableName t = new TableName(catName, dbName, tableName);
      GetPartitionsHandler<PartitionsRequest, Partition> getPartitionsHandler = AbstractRequestHandler.offer(this,
          new GetPartitionsHandler.GetPartitionsRequest<>(GetPartitionsHandler.createPartitionsRequest(t, max_parts), t));
      List<Partition> partitions  = getPartitionsHandler.getResult().result();
      if (is_partition_spec_grouping_enabled(table)) {
        partitionSpecs = MetaStoreServerUtils
            .getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      }
      else {
        PartitionSpec pSpec = new PartitionSpec();
        pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
        pSpec.setCatName(normalizeIdentifier(catName));
        pSpec.setDbName(normalizeIdentifier(dbName));
        pSpec.setTableName(tableName);
        pSpec.setRootPath(table.getSd().getLocation());
        partitionSpecs = Arrays.asList(pSpec);
      }

      return partitionSpecs;
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(NoSuchObjectException.class, MetaException.class).defaultMetaException();
    }
    finally {
      endFunction("get_partitions_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), null, tbl_name);
    }
  }

  @Override
  @Deprecated
  public List<String> get_partition_names(final String db_name, final String tbl_name,
      final short max_parts) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    PartitionsRequest partitionReq = new PartitionsRequest(parsedDbName[DB_NAME], tbl_name);
    partitionReq.setCatName(parsedDbName[CAT_NAME]);
    partitionReq.setMaxParts(max_parts);
    return fetch_partition_names_req(partitionReq);
  }

  @Deprecated
  @Override
  public void alter_partition(final String db_name, final String tbl_name,
      final Partition new_part)
      throws TException {
    rename_partition(db_name, tbl_name, null, new_part);
  }

  @Deprecated
  @Override
  public void alter_partition_with_environment_context(final String dbName,
      final String tableName, final Partition newPartition,
      final EnvironmentContext envContext)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    alter_partition_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, null,
        newPartition, envContext, null);
  }

  @Deprecated
  @Override
  public void rename_partition(final String db_name, final String tbl_name,
      final List<String> part_vals, final Partition new_part)
      throws TException {
    // Call alter_partition_core without an environment context.
    String[] parsedDbName = parseDbName(db_name, conf);
    alter_partition_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_vals, new_part,
        null, null);
  }

  protected void alter_partition_core(String catName, String db_name, String tbl_name,
      List<String> part_vals, Partition new_part, EnvironmentContext envContext,
      String validWriteIds) throws TException {
    startTableFunction("alter_partition", catName, db_name, tbl_name);

    if (LOG.isInfoEnabled()) {
      LOG.info("New partition values:" + new_part.getValues());
      if (part_vals != null && part_vals.size() > 0) {
        LOG.info("Old Partition values:" + part_vals);
      }
    }

    // Adds the missing scheme/authority for the new partition location
    if (new_part.getSd() != null) {
      String newLocation = new_part.getSd().getLocation();
      if (org.apache.commons.lang3.StringUtils.isNotEmpty(newLocation)) {
        Path tblPath = wh.getDnsPath(new Path(newLocation));
        new_part.getSd().setLocation(tblPath.toString());
      }
    }

    // Make sure the new partition has the catalog value set
    if (!new_part.isSetCatName()) {
      new_part.setCatName(catName);
    }

    Partition oldPart = null;
    Exception ex = null;
    try {
      Table table = getMS().getTable(catName, db_name, tbl_name, null);

      firePreEvent(new PreAlterPartitionEvent(db_name, tbl_name, table, part_vals, new_part, this));
      if (part_vals != null && !part_vals.isEmpty()) {
        MetaStoreServerUtils.validatePartitionNameCharacters(new_part.getValues(), getConf());
      }

      oldPart = alterHandler.alterPartition(getMS(), wh, catName, db_name, tbl_name,
          part_vals, new_part, envContext, this, validWriteIds);

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventMessage.EventType.ALTER_PARTITION,
            new AlterPartitionEvent(oldPart, new_part, table, false,
                true, new_part.getWriteId(), this),
            envContext);
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .convertIfInstance(InvalidObjectException.class, InvalidOperationException.class)
          .convertIfInstance(AlreadyExistsException.class, InvalidOperationException.class)
          .defaultMetaException();
    } finally {
      endFunction("alter_partition", oldPart != null, ex, tbl_name);
    }
  }

  @Deprecated
  @Override
  public void alter_partitions(final String db_name, final String tbl_name,
      final List<Partition> new_parts)
      throws TException {
    String[] o = parseDbName(db_name, conf);
    AlterPartitionsRequest req = new AlterPartitionsRequest(o[1], tbl_name, new_parts);
    req.setCatName(o[0]);
    alter_partitions_req(req);
  }

  // The old API we are keeping for backward compat. Not used within Hive.
  @Deprecated
  @Override
  public void alter_partitions_with_environment_context(final String db_name, final String tbl_name,
      final List<Partition> new_parts, EnvironmentContext environmentContext)
      throws TException {
    String[] o = parseDbName(db_name, conf);
    AlterPartitionsRequest req = new AlterPartitionsRequest(o[1], tbl_name, new_parts);
    req.setCatName(o[0]);
    req.setEnvironmentContext(environmentContext);
    alter_partitions_req(req);
  }

  @Deprecated
  @Override
  public void alter_table(final String dbname, final String name,
      final Table newTable) throws TException {
    // Do not set an environment context.
    String[] parsedDbName = parseDbName(dbname, conf);
    AlterTableRequest req = new AlterTableRequest(parsedDbName[DB_NAME], name, newTable);
    req.setCatName(parsedDbName[CAT_NAME]);
    alter_table_req(req);
  }

  @Deprecated
  @Override
  public void alter_table_with_cascade(final String dbname, final String name,
      final Table newTable, final boolean cascade) throws TException {
    EnvironmentContext envContext = null;
    if (cascade) {
      envContext = new EnvironmentContext();
      envContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
    }
    String[] parsedDbName = parseDbName(dbname, conf);
    AlterTableRequest req = new AlterTableRequest(parsedDbName[DB_NAME], name, newTable);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvironmentContext(envContext);
    alter_table_req(req);
  }

  @Deprecated
  @Override
  public void alter_table_with_environment_context(final String dbname,
      final String name, final Table newTable,
      final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(dbname, conf);
    AlterTableRequest req = new AlterTableRequest(parsedDbName[DB_NAME], name, newTable);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvironmentContext(envContext);
    alter_table_req(req);
  }

  /**
   * Use {@link #get_fields_req(GetFieldsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<FieldSchema> get_fields(String db, String tableName) throws TException {
    return get_fields_with_environment_context(db, tableName, null);
  }

  @Override
  @Deprecated
  public List<FieldSchema> get_fields_with_environment_context(String db, String tableName,
      final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(db, conf);
    GetFieldsRequest req = new GetFieldsRequest(parsedDbName[DB_NAME], tableName);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvContext(envContext);
    return get_fields_req(req).getFields();
  }


  /**
   * Use {@link #get_schema_req(GetSchemaRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<FieldSchema> get_schema(String db, String tableName) throws TException {
    return get_schema_with_environment_context(db,tableName, null);
  }

  /**
   * Return the schema of the table. This function includes partition columns
   * in addition to the regular columns.
   *
   * @param db
   *          Name of the database
   * @param tableName
   *          Name of the table
   * @param envContext
   *          Store session based properties
   * @return List of columns, each column is a FieldSchema structure
   * @throws MetaException
   * @throws UnknownTableException
   * @throws UnknownDBException
   */
  @Override
  @Deprecated
  public List<FieldSchema> get_schema_with_environment_context(String db, String tableName,
      final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(db, conf);
    GetSchemaRequest req = new GetSchemaRequest(parsedDbName[DB_NAME], tableName);
    req.setCatName(parsedDbName[CAT_NAME]);
    req.setEnvContext(envContext);
    return get_schema_req(req).getFields();
  }

  @Override
  @Deprecated
  public Partition get_partition_by_name(final String db_name, final String tbl_name,
      final String part_name) throws TException {
    if (StringUtils.isBlank(part_name)) {
      throw new MetaException("The part_name in get_partition_by_name cannot be null or empty");
    }
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    GetPartitionsByNamesRequest gpnr = new GetPartitionsByNamesRequest(tableName.getDb(), tableName.getTable());
    gpnr.setNames(List.of(part_name));
    List<Partition> partitions = GetPartitionsHandler.getPartitions(
        t ->  startFunction("get_partition_by_name", ": tbl=" + t + " part=" + part_name),
        rex -> endFunction("get_partition_by_name",
            rex.getLeft() != null && rex.getLeft().success(), rex.getRight(), tableName.toString()),
        this, tableName, gpnr, true);
    return partitions.getFirst();
  }

  @Deprecated
  @Override
  public Partition append_partition_by_name(final String db_name, final String tbl_name,
      final String part_name) throws TException {
    return append_partition_by_name_with_environment_context(db_name, tbl_name, part_name, null);
  }

  @Deprecated
  @Override
  public Partition append_partition_by_name_with_environment_context(final String db_name,
      final String tbl_name, final String part_name, final EnvironmentContext env_context)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    AppendPartitionsRequest appendPartitionRequest = new AppendPartitionsRequest();
    appendPartitionRequest.setDbName(parsedDbName[DB_NAME]);
    appendPartitionRequest.setTableName(tbl_name);
    appendPartitionRequest.setName(part_name);
    appendPartitionRequest.setCatalogName(parsedDbName[CAT_NAME]);
    appendPartitionRequest.setEnvironmentContext(env_context);
    return append_partition_req(appendPartitionRequest);
  }

  @Deprecated
  @Override
  public boolean drop_partition_by_name(final String db_name, final String tbl_name,
      final String part_name, final boolean deleteData) throws TException {
    return drop_partition_by_name_with_environment_context(db_name, tbl_name, part_name,
        deleteData, null);
  }

  @Deprecated
  @Override
  public boolean drop_partition_by_name_with_environment_context(final String db_name,
      final String tbl_name, final String part_name, final boolean deleteData,
      final EnvironmentContext envContext) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    DropPartitionRequest dropPartitionReq = new DropPartitionRequest(parsedDbName[DB_NAME], tbl_name);
    dropPartitionReq.setCatName(parsedDbName[CAT_NAME]);
    dropPartitionReq.setPartName(part_name);
    dropPartitionReq.setDeleteData(deleteData);
    dropPartitionReq.setEnvironmentContext(envContext);
    return drop_partition_req(dropPartitionReq);
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_ps(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final short max_parts) throws TException {
    return get_partitions_ps_with_auth(db_name, tbl_name, part_vals, max_parts, null, null);
  }

  /**
   * Use {@link #get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<Partition> get_partitions_ps_with_auth(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final short max_parts, final String userName,
      final List<String> groupNames) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    TableName tableName = new TableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    GetPartitionsPsWithAuthRequest getAuthReq = new GetPartitionsPsWithAuthRequest(tableName.getDb(), tableName.getTable());
    getAuthReq.setCatName(tableName.getCat());
    getAuthReq.setMaxParts(max_parts);
    getAuthReq.setUserName(userName);
    getAuthReq.setGroupNames(groupNames);
    getAuthReq.setPartVals(part_vals);
    return get_partitions_ps_with_auth_req(getAuthReq).getPartitions();
  }

  /**
   * Use {@link #get_partition_names_ps_req(GetPartitionNamesPsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<String> get_partition_names_ps(final String db_name,
      final String tbl_name, final List<String> part_vals, final short max_parts)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("get_partitions_names_ps", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
        max_parts, part_vals);

    List<String> ret = null;
    Exception ex = null;
    try {
      GetPartitionNamesPsRequest getPartitionNamesPsRequest = new GetPartitionNamesPsRequest(parsedDbName[DB_NAME], tbl_name);
      getPartitionNamesPsRequest.setCatName(parsedDbName[CAT_NAME]);
      getPartitionNamesPsRequest.setPartValues(part_vals);
      getPartitionNamesPsRequest.setMaxParts(max_parts);
      ret = get_partition_names_ps_req(getPartitionNamesPsRequest).getNames();
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_names_ps", ret != null, ex, tbl_name);
    }
    return ret;
  }

  @Override
  @Deprecated
  public ColumnStatistics get_table_column_statistics(String dbName, String tableName,
      String colName) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    parsedDbName[CAT_NAME] = parsedDbName[CAT_NAME].toLowerCase();
    parsedDbName[DB_NAME] = parsedDbName[DB_NAME].toLowerCase();
    tableName = tableName.toLowerCase();
    colName = colName.toLowerCase();
    startFunction("get_column_statistics_by_table", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tableName) + " column=" + colName);
    ColumnStatistics statsObj = null;
    try {
      statsObj = getMS().getTableColumnStatistics(
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, Lists.newArrayList(colName),
          "hive", null);
      if (statsObj != null) {
        assert statsObj.getStatsObjSize() <= 1;
      }
      return statsObj;
    } finally {
      endFunction("get_column_statistics_by_table", statsObj != null, null, tableName);
    }
  }

  @Override
  @Deprecated
  public ColumnStatistics get_partition_column_statistics(String dbName, String tableName,
      String partName, String colName) throws TException {
    // Note: this method appears to be unused within Hive.
    //       It doesn't take txn stats into account.
    dbName = dbName.toLowerCase();
    String[] parsedDbName = parseDbName(dbName, conf);
    tableName = tableName.toLowerCase();
    colName = colName.toLowerCase();
    startFunction("get_column_statistics_by_partition", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tableName) + " partition=" + partName + " column=" + colName);
    ColumnStatistics statsObj = null;

    try {
      List<ColumnStatistics> list = getMS().getPartitionColumnStatistics(
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
          Lists.newArrayList(partName), Lists.newArrayList(colName),
          "hive");
      if (list.isEmpty()) {
        return null;
      }
      if (list.size() != 1) {
        throw new MetaException(list.size() + " statistics for single column and partition");
      }
      statsObj = list.get(0);
    } finally {
      endFunction("get_column_statistics_by_partition", statsObj != null, null, tableName);
    }
    return statsObj;
  }

  @Deprecated
  @Override
  public boolean delete_partition_column_statistics(String dbName, String tableName,
      String partName, String colName, String engine) throws TException {
    dbName = dbName.toLowerCase();
    String[] parsedDbName = parseDbName(dbName, conf);
    tableName = tableName.toLowerCase();
    if (colName != null) {
      colName = colName.toLowerCase();
    }
    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(parsedDbName[DB_NAME], tableName);
    request.setEngine(engine);
    request.setCat_name(parsedDbName[CAT_NAME]);
    request.addToCol_names(colName);
    request.addToPart_names(partName);
    return delete_column_statistics_req(request);
  }

  @Deprecated
  @Override
  public boolean delete_table_column_statistics(String dbName, String tableName, String colName, String engine)
      throws TException {
    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    String[] parsedDbName = parseDbName(dbName, conf);

    if (colName != null) {
      colName = colName.toLowerCase();
    }

    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(parsedDbName[DB_NAME], tableName);
    request.setEngine(engine);
    request.setCat_name(parsedDbName[CAT_NAME]);
    request.addToCol_names(colName);
    request.setTableLevel(true);
    return delete_column_statistics_req(request);
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_by_filter(final String dbName, final String tblName,
      final String filter, final short maxParts)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    GetPartitionsByFilterRequest gfr =
        new GetPartitionsByFilterRequest(parsedDbName[DB_NAME], tblName, filter);
    gfr.setMaxParts(maxParts);
    gfr.setCatName(parsedDbName[CAT_NAME]);
    return get_partitions_by_filter_req(gfr);
  }

  @Override
  @Deprecated
  public List<PartitionSpec> get_part_specs_by_filter(final String dbName, final String tblName,
      final String filter, final int maxParts)
      throws TException {

    String[] parsedDbName = parseDbName(dbName, conf);
    startPartitionFunction("get_partitions_by_filter_pspec", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName,
        maxParts, filter);
    List<PartitionSpec> partitionSpecs = null;
    try {
      GetTableRequest getTableRequest = new GetTableRequest(parsedDbName[DB_NAME], tblName);
      getTableRequest.setCatName(parsedDbName[CAT_NAME]);
      Table table = get_table_core(getTableRequest);
      // Don't pass the parsed db name, as get_partitions_by_filter will parse it itself
      List<Partition> partitions = get_partitions_by_filter(dbName, tblName, filter, (short) maxParts);

      if (is_partition_spec_grouping_enabled(table)) {
        partitionSpecs = MetaStoreServerUtils
            .getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      }
      else {
        PartitionSpec pSpec = new PartitionSpec();
        pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
        pSpec.setRootPath(table.getSd().getLocation());
        pSpec.setCatName(parsedDbName[CAT_NAME]);
        pSpec.setDbName(parsedDbName[DB_NAME]);
        pSpec.setTableName(tblName);
        partitionSpecs = Arrays.asList(pSpec);
      }

      return partitionSpecs;
    }
    finally {
      endFunction("get_partitions_by_filter_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), null, tblName);
    }
  }

  @Override
  @Deprecated
  public int get_num_partitions_by_filter(final String dbName,
      final String tblName, final String filter)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    if (parsedDbName[DB_NAME] == null || tblName == null) {
      throw new MetaException("The DB and table name cannot be null.");
    }
    startFunction("get_num_partitions_by_filter",
        " : tbl=" + TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName) + " Filter="
            + filter);

    int ret = -1;
    Exception ex = null;
    try {
      ret = getMS().getNumPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tblName, filter);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_num_partitions_by_filter", ret != -1, ex, tblName);
    }
    return ret;
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_by_names(final String dbName, final String tblName,
      final List<String> partNames)
      throws TException {
    if (partNames == null) {
      throw new MetaException("The partNames is null");
    }
    GetPartitionsByNamesRequest request = new GetPartitionsByNamesRequest(dbName, tblName);
    request.setNames(partNames);
    return get_partitions_by_names_req(request).getPartitions();
  }

  @Override
  public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req)
      throws TException {
    GetFileMetadataByExprResult result = new GetFileMetadataByExprResult();
    RawStore ms = getMS();
    if (!ms.isFileMetadataSupported()) {
      result.setIsSupported(false);
      result.setMetadata(Collections.emptyMap()); // Set the required field.
      return result;
    }
    result.setIsSupported(true);

    List<Long> fileIds = req.getFileIds();
    boolean needMetadata = !req.isSetDoGetFooters() || req.isDoGetFooters();
    FileMetadataExprType type = req.isSetType() ? req.getType() : FileMetadataExprType.ORC_SARG;

    ByteBuffer[] metadatas = needMetadata ? new ByteBuffer[fileIds.size()] : null;
    ByteBuffer[] ppdResults = new ByteBuffer[fileIds.size()];
    boolean[] eliminated = new boolean[fileIds.size()];

    getMS().getFileMetadataByExpr(fileIds, type, req.getExpr(), metadatas, ppdResults, eliminated);
    for (int i = 0; i < fileIds.size(); ++i) {
      if (!eliminated[i] && ppdResults[i] == null)
      {
        continue; // No metadata => no ppd.
      }
      MetadataPpdResult mpr = new MetadataPpdResult();
      ByteBuffer ppdResult = eliminated[i] ? null : handleReadOnlyBufferForThrift(ppdResults[i]);
      mpr.setIncludeBitset(ppdResult);
      if (needMetadata) {
        ByteBuffer metadata = eliminated[i] ? null : handleReadOnlyBufferForThrift(metadatas[i]);
        mpr.setMetadata(metadata);
      }
      result.putToMetadata(fileIds.get(i), mpr);
    }
    if (!result.isSetMetadata()) {
      result.setMetadata(Collections.emptyMap()); // Set the required field.
    }
    return result;
  }

  @Override
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
    GetFileMetadataResult result = new GetFileMetadataResult();
    RawStore ms = getMS();
    if (!ms.isFileMetadataSupported()) {
      result.setIsSupported(false);
      result.setMetadata(Collections.emptyMap()); // Set the required field.
      return result;
    }
    result.setIsSupported(true);
    List<Long> fileIds = req.getFileIds();
    ByteBuffer[] metadatas = ms.getFileMetadata(fileIds);
    assert metadatas.length == fileIds.size();
    for (int i = 0; i < metadatas.length; ++i) {
      ByteBuffer bb = metadatas[i];
      if (bb == null) {
        continue;
      }
      bb = handleReadOnlyBufferForThrift(bb);
      result.putToMetadata(fileIds.get(i), bb);
    }
    if (!result.isSetMetadata()) {
      result.setMetadata(Collections.emptyMap()); // Set the required field.
    }
    return result;
  }

  private ByteBuffer handleReadOnlyBufferForThrift(ByteBuffer bb) {
    if (!bb.isReadOnly()) {
      return bb;
    }
    // Thrift cannot write read-only buffers... oh well.
    // TODO: actually thrift never writes to the buffer, so we could use reflection to
    //       unset the unnecessary read-only flag if allocation/copy perf becomes a problem.
    ByteBuffer copy = ByteBuffer.allocate(bb.capacity());
    copy.put(bb);
    copy.flip();
    return copy;
  }

  @Override
  public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
    RawStore ms = getMS();
    if (ms.isFileMetadataSupported()) {
      ms.putFileMetadata(req.getFileIds(), req.getMetadata(), req.getType());
    }
    return new PutFileMetadataResult();
  }

  @Override
  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req)
      throws TException {
    getMS().putFileMetadata(req.getFileIds(), null, null);
    return new ClearFileMetadataResult();
  }

  @Override
  public CacheFileMetadataResult cache_file_metadata(
      CacheFileMetadataRequest req) throws TException {
    RawStore ms = getMS();
    if (!ms.isFileMetadataSupported()) {
      return new CacheFileMetadataResult(false);
    }
    String dbName = req.getDbName(), tblName = req.getTblName(),
        partName = req.isSetPartName() ? req.getPartName() : null;
    boolean isAllPart = req.isSetIsAllParts() && req.isIsAllParts();
    ms.openTransaction();
    boolean success = false;
    try {
      Table tbl = ms.getTable(getDefaultCatalog(conf), dbName, tblName);
      if (tbl == null) {
        throw new NoSuchObjectException(dbName + "." + tblName + " not found");
      }
      boolean isPartitioned = tbl.isSetPartitionKeys() && tbl.getPartitionKeysSize() > 0;
      String tableInputFormat = tbl.isSetSd() ? tbl.getSd().getInputFormat() : null;
      if (!isPartitioned) {
        if (partName != null || isAllPart) {
          throw new MetaException("Table is not partitioned");
        }
        if (!tbl.isSetSd() || !tbl.getSd().isSetLocation()) {
          throw new MetaException(
              "Table does not have storage location; this operation is not supported on views");
        }
        FileMetadataExprType type = expressionProxy.getMetadataType(tableInputFormat);
        if (type == null) {
          throw new MetaException("The operation is not supported for " + tableInputFormat);
        }
        fileMetadataManager.queueCacheMetadata(tbl.getSd().getLocation(), type);
        success = true;
      } else {
        List<String> partNames;
        if (partName != null) {
          partNames = Lists.newArrayList(partName);
        } else if (isAllPart) {
          partNames = ms.listPartitionNames(getDefaultCatalog(conf), dbName, tblName, (short)-1);
        } else {
          throw new MetaException("Table is partitioned");
        }
        int batchSize = MetastoreConf.getIntVar(
            conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);
        int index = 0;
        int successCount = 0, failCount = 0;
        HashSet<String> failFormats = null;
        while (index < partNames.size()) {
          int currentBatchSize = Math.min(batchSize, partNames.size() - index);
          List<String> nameBatch = partNames.subList(index, index + currentBatchSize);
          index += currentBatchSize;
          List<Partition> parts = ms.getPartitionsByNames(getDefaultCatalog(conf), dbName, tblName, nameBatch);
          for (Partition part : parts) {
            if (!part.isSetSd() || !part.getSd().isSetLocation()) {
              throw new MetaException("Partition does not have storage location;" +
                  " this operation is not supported on views");
            }
            String inputFormat = part.getSd().isSetInputFormat()
                ? part.getSd().getInputFormat() : tableInputFormat;
            FileMetadataExprType type = expressionProxy.getMetadataType(inputFormat);
            if (type == null) {
              ++failCount;
              if (failFormats == null) {
                failFormats = new HashSet<>();
              }
              failFormats.add(inputFormat);
            } else {
              ++successCount;
              fileMetadataManager.queueCacheMetadata(part.getSd().getLocation(), type);
            }
          }
        }
        success = true; // Regardless of the following exception
        if (failCount > 0) {
          String errorMsg = "The operation failed for " + failCount + " partitions and "
              + "succeeded for " + successCount + " partitions; unsupported formats: ";
          boolean isFirst = true;
          for (String s : failFormats) {
            if (!isFirst) {
              errorMsg += ", ";
            }
            isFirst = false;
            errorMsg += s;
          }
          throw new MetaException(errorMsg);
        }
      }
    } finally {
      if (success) {
        if (!ms.commitTransaction()) {
          throw new MetaException("Failed to commit");
        }
      } else {
        ms.rollbackTransaction();
      }
    }
    return new CacheFileMetadataResult(true);
  }

  private static boolean is_partition_spec_grouping_enabled(Table table) {
    Map<String, String> parameters = table.getParameters();
    return parameters.containsKey("hive.hcatalog.partition.spec.grouping.enabled")
        && parameters.get("hive.hcatalog.partition.spec.grouping.enabled").equalsIgnoreCase("true");
  }
}
