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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.AcidMetaDataFile.DataFormat;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionsEvent;
import org.apache.hadoop.hive.metastore.handler.AlterTableHandler;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.LinkedList;
import java.util.Optional;

import static org.apache.hadoop.hive.metastore.HiveMetaHook.ALTERLOCATION;
import static org.apache.hadoop.hive.metastore.HiveMetaHook.ALTER_TABLE_OPERATION_TYPE;
import static org.apache.hadoop.hive.metastore.HiveMetaStoreClient.RENAME_PARTITION_MAKE_COPY;
import static org.apache.hadoop.hive.metastore.handler.TruncateTableHandler.addTruncateBaseFile;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.findStaleColumns;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * Hive specific implementation of alter
 */
public class HiveAlterHandler implements AlterHandler {

  protected Configuration conf;
  private static final Logger LOG = LoggerFactory.getLogger(HiveAlterHandler.class
      .getName());

  // hiveConf, getConf and setConf are in this class because AlterHandler extends Configurable.
  // Always use the configuration from HMS Handler.  Making AlterHandler not extend Configurable
  // is not in the scope of the fix for HIVE-17942.
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void alterTable(RawStore msdb, Warehouse wh, String catName, String dbname,
      String name, Table newt, EnvironmentContext environmentContext,
      IHMSHandler handler, String writeIdList)
          throws InvalidOperationException, MetaException {
    AlterTableHandler.runDirectAlter(handler,
        new AlterTableHandler.DirectAlterContext(msdb, wh, catName, dbname, name, newt,
            environmentContext, writeIdList));
  }

  /**
   * MetaException that encapsulates error message from RemoteException from hadoop RPC which wrap
   * the stack trace into e.getMessage() which makes logs/stack traces confusing.
   * @param ex
   * @return
   */
  public static String getSimpleMessage(Exception ex) {
    if(ex instanceof MetaException) {
      String msg = ex.getMessage();
      if(msg == null || !msg.contains("\n")) {
        return msg;
      }
      return msg.substring(0, msg.indexOf('\n'));
    }
    return ex.getMessage();
  }

  @Override
  public Partition alterPartition(RawStore msdb, Warehouse wh, String catName, String dbname,
      String name, List<String> part_vals, final Partition new_part,
      EnvironmentContext environmentContext, IHMSHandler handler, String validWriteIds)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException, NoSuchObjectException {
    boolean success = false;
    Partition oldPart;
    List<TransactionalMetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    // Set DDL time to now if not specified
    if (new_part.getParameters() == null ||
        new_part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
        Integer.parseInt(new_part.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
      new_part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
    }

    //alter partition
    if (CollectionUtils.isEmpty(part_vals)) {
      try {
        msdb.openTransaction();

        Table tbl = msdb.getTable(catName, dbname, name, null);
        if (tbl == null) {
          throw new InvalidObjectException(
              "Unable to alter partition because table or database does not exist.");
        }
        oldPart = msdb.getPartition(catName, dbname, name, new_part.getValues());
        if (MetaStoreServerUtils.requireCalStats(oldPart, new_part, tbl, environmentContext)) {
          // if stats are same, no need to update
          if (MetaStoreServerUtils.isFastStatsSame(oldPart, new_part)) {
            MetaStoreServerUtils.updateBasicState(environmentContext, new_part.getParameters());
          } else {
            MetaStoreServerUtils.updatePartitionStatsFast(
                new_part, tbl, wh, false, true, environmentContext, false);
          }
        }

        // PartitionView does not have SD. We do not need update its column stats
        if (oldPart.getSd() != null) {
          updateOrGetPartitionColumnStats(msdb, catName, dbname, name, new_part.getValues(),
              oldPart.getSd().getCols(), tbl, new_part, null);
        }
        Deadline.checkTimeout();
        msdb.alterPartition(
            catName, dbname, name, new_part.getValues(), new_part, validWriteIds);
        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                EventMessage.EventType.ALTER_PARTITION,
                                                new AlterPartitionEvent(oldPart, new_part, tbl, false,
                                                        true, new_part.getWriteId(), handler),
                                                environmentContext);


        }
        success = msdb.commitTransaction();
      } catch (InvalidObjectException e) {
        LOG.warn("Alter failed", e);
        throw new InvalidOperationException("alter is not possible: " + e.getMessage());
      } catch (NoSuchObjectException e) {
        //old partition does not exist
        throw new InvalidOperationException("alter is not possible: " + e.getMessage());
      } finally {
        if(!success) {
          msdb.rollbackTransaction();
        }
      }
      return oldPart;
    }

    //rename partition
    String oldPartLoc;
    String newPartLoc;
    Path srcPath = null;
    Path destPath = null;
    FileSystem srcFs;
    FileSystem destFs = null;
    boolean dataWasMoved = false;
    Database db;
    Partition check_part;
    try {
      check_part = msdb.getPartition(catName, dbname, name, new_part.getValues());
    } catch(NoSuchObjectException e) {
      // this means there is no existing partition
      check_part = null;
    }

    if (check_part != null) {
      throw new AlreadyExistsException("Partition already exists:" + dbname + "." + name + "." +
          new_part.getValues());
    }
    try {
      msdb.openTransaction();
      Table tbl = msdb.getTable(catName, dbname, name, null);
      if (tbl == null) {
        throw new InvalidObjectException(
            "Unable to alter partition because table or database does not exist.");
      }
      MTable mTable = msdb.ensureGetMTable(catName, dbname, name);
      try {
        oldPart = msdb.getPartition(catName, dbname, name, part_vals);
      } catch (NoSuchObjectException e) {
        // this means there is no existing partition
        throw new InvalidObjectException(
            "Unable to rename partition because old partition does not exist");
      }

      // when renaming a partition, we should update
      // 1) partition SD Location
      // 2) partition column stats if there are any because of part_name field in HMS table PART_COL_STATS
      // 3) rename the partition directory if it is not an external table
      boolean shouldMoveData = !(MetaStoreUtils.isExternalTable(tbl) &&
          !MetaStoreUtils.isPropertyTrue(tbl.getParameters(), "TRANSLATED_TO_EXTERNAL"));
      if (shouldMoveData) {
        // TODO: refactor this into a separate method after master merge, this one is too big.
        try {
          db = msdb.getDatabase(catName, dbname);

          // if tbl location is available use it
          // else derive the tbl location from database location
          destPath = wh.getPartitionPath(db, tbl, new_part.getValues());
          destPath = constructRenamedPath(destPath, new Path(new_part.getSd().getLocation()));
        } catch (NoSuchObjectException e) {
          LOG.debug("Didn't find object in metastore ", e);
          throw new InvalidOperationException(
            "Unable to change partition or table. Database " + dbname + " does not exist"
              + " Check metastore logs for detailed stack." + e.getMessage());
        }

        newPartLoc = destPath.toString();
        oldPartLoc = oldPart.getSd().getLocation();
        LOG.info("srcPath:" + oldPartLoc);
        LOG.info("descPath:" + newPartLoc);
        srcPath = new Path(oldPartLoc);
        srcFs = wh.getFs(srcPath);
        destFs = wh.getFs(destPath);
        // check that src and dest are on the same file system
        if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
          throw new InvalidOperationException("New table location " + destPath
            + " is on a different file system than the old location "
            + srcPath + ". This operation is not supported.");
        }

        try {
          if (srcFs.exists(srcPath)) {
            if (newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
              throw new InvalidOperationException("New location for this table "
                + tbl.getDbName() + "." + tbl.getTableName()
                + " already exists : " + destPath);
            }
            //if destPath's parent path doesn't exist, we should mkdir it
            Path destParentPath = destPath.getParent();
            if (!wh.mkdirs(destParentPath)) {
                throw new MetaException("Unable to create path " + destParentPath);
            }

            boolean clonePart = Optional.ofNullable(environmentContext)
                .map(EnvironmentContext::getProperties)
                .map(prop -> prop.get(RENAME_PARTITION_MAKE_COPY))
                .map(Boolean::parseBoolean)
                .orElse(false);
            long writeId = new_part.getWriteId();

            if (writeId > 0 && clonePart) {
              LOG.debug("Making a copy of the partition directory: {} under a new location: {}", srcPath, destPath);

              if (!wh.copyDir(srcPath, destPath, ReplChangeManager.shouldEnableCm(db, tbl))) {
                LOG.error("Copy failed for source: " + srcPath + " to destination: " + destPath);
                throw new IOException("File copy failed.");
              }
              addTruncateBaseFile(srcPath, writeId, conf, DataFormat.DROPPED);
            } else {
              //rename the data directory
              wh.renameDir(srcPath, destPath, ReplChangeManager.shouldEnableCm(db, tbl));
            }
            LOG.info("Partition directory rename from " + srcPath + " to " + destPath + " done.");
            dataWasMoved = true;
          }
        } catch (IOException e) {
          LOG.error("Cannot rename partition directory from " + srcPath + " to " + destPath, e);
          throw new InvalidOperationException("Unable to access src or dest location for partition "
              + tbl.getDbName() + "." + tbl.getTableName() + " " + new_part.getValues());
        } catch (MetaException me) {
          LOG.error("Cannot rename partition directory from " + srcPath + " to " + destPath, me);
          throw me;
        }
        new_part.getSd().setLocation(newPartLoc);
      } else {
        new_part.getSd().setLocation(oldPart.getSd().getLocation());
      }

      if (MetaStoreServerUtils.requireCalStats(oldPart, new_part, tbl, environmentContext)) {
        MetaStoreServerUtils.updatePartitionStatsFast(
            new_part, tbl, wh, false, true, environmentContext, false);
      }

      updateOrGetPartitionColumnStats(msdb, catName, dbname, name, oldPart.getValues(),
          oldPart.getSd().getCols(), tbl, new_part, null);

      msdb.alterPartition(catName, dbname, name, part_vals, new_part, validWriteIds);
      if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                              EventMessage.EventType.ALTER_PARTITION,
                                              new AlterPartitionEvent(oldPart, new_part, tbl, false,
                                                      true, new_part.getWriteId(), handler),
                                              environmentContext);
      }

      success = msdb.commitTransaction();
    } finally {
      if (!success) {
        LOG.error("Failed to rename a partition. Rollback transaction");
        msdb.rollbackTransaction();
        if (dataWasMoved) {
          LOG.error("Revert the data move in renaming a partition.");
          try {
            if (destFs.exists(destPath)) {
              wh.renameDir(destPath, srcPath, false);
            }
          } catch (MetaException | IOException me) {
            LOG.error("Failed to restore partition data from " + destPath + " to " + srcPath
                +  " in alter partition failure. Manual restore is needed.");
          }
        }
      }
    }
    return oldPart;
  }

  private Map<List<String>, Partition> getExistingPartitions(final RawStore msdb,
      final List<Partition> new_parts, final Table tbl, final String catName,
      final String dbname, final String name)
      throws MetaException, NoSuchObjectException, InvalidOperationException {

    // Get list of partition values
    List<String> partValues = new LinkedList<>();
    for (Partition tmpPart : new_parts) {
      partValues.add(Warehouse.makePartName(tbl.getPartitionKeys(), tmpPart.getValues()));
    }

    // Get existing partitions from store
    List<Partition> oldParts = msdb.getPartitionsByNames(catName, dbname, name, partValues);
    if (new_parts.size() != oldParts.size()) {
      throw new InvalidOperationException("Alter partition operation failed: "
          + "new parts size " + new_parts.size()
          + " not matching with old parts size " + oldParts.size());
    }
    return oldParts.stream().collect(Collectors.toMap(Partition::getValues, Partition -> Partition));
  }

  @Override
  public List<Partition> alterPartitions(final RawStore msdb, Warehouse wh, final String catName,
      final String dbname, final String name, final List<Partition> new_parts,
      EnvironmentContext environmentContext, String writeIdList, long writeId,
      IHMSHandler handler)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    List<Partition> oldParts = new ArrayList<>();
    List<List<String>> partValsList = new ArrayList<>();
    List<TransactionalMetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    boolean success = false;
    try {
      msdb.openTransaction();

      // Note: should we pass in write ID here? We only update stats on parts so probably not.
      Table tbl = msdb.getTable(catName, dbname, name, null);
      if (tbl == null) {
        throw new InvalidObjectException(
            "Unable to alter partitions because table or database does not exist.");
      }

      blockPartitionLocationChangesOnReplSource(msdb.getDatabase(catName, dbname), tbl,
                                                environmentContext);
      Map<List<String>, Partition> oldPartMap = getExistingPartitions(msdb, new_parts, tbl, catName, dbname, name);

      for (Partition tmpPart: new_parts) {
        // Set DDL time to now if not specified
        if (tmpPart.getParameters() == null ||
            tmpPart.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
            Integer.parseInt(tmpPart.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
          tmpPart.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
              .currentTimeMillis() / 1000));
        }

        Partition oldTmpPart = oldPartMap.get(tmpPart.getValues());
        oldParts.add(oldTmpPart);
        partValsList.add(tmpPart.getValues());

        if (MetaStoreServerUtils.requireCalStats(oldTmpPart, tmpPart, tbl, environmentContext)) {
          // Check if stats are same, no need to update
          if (MetaStoreServerUtils.isFastStatsSame(oldTmpPart, tmpPart)) {
            MetaStoreServerUtils.updateBasicState(environmentContext, tmpPart.getParameters());
          } else {
            MetaStoreServerUtils.updatePartitionStatsFast(
                tmpPart, tbl, wh, false, true, environmentContext, false);
          }
        }

        // PartitionView does not have SD and we do not need to update its column stats
        if (oldTmpPart.getSd() != null) {
          updateOrGetPartitionColumnStats(msdb, catName, dbname, name, oldTmpPart.getValues(),
              oldTmpPart.getSd().getCols(), tbl, tmpPart, null);
        }
      }

      msdb.alterPartitions(catName, dbname, name, partValsList, new_parts, writeId, writeIdList);

      if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
        boolean shouldSendSingleEvent = MetastoreConf.getBoolVar(handler.getConf(),
            MetastoreConf.ConfVars.NOTIFICATION_ALTER_PARTITIONS_V2_ENABLED);
        if (shouldSendSingleEvent) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ALTER_PARTITIONS,
              new AlterPartitionsEvent(oldParts, new_parts, tbl, false, true, handler), environmentContext);
        } else {
          for (Partition newPart : new_parts) {
            Partition oldPart = oldPartMap.get(newPart.getValues());
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ALTER_PARTITION,
                new AlterPartitionEvent(oldPart, newPart, tbl, false, true, newPart.getWriteId(), handler),
                environmentContext);
          }
        }
      }
      success = msdb.commitTransaction();
    } catch (InvalidObjectException | NoSuchObjectException e) {
      throw new InvalidOperationException("Alter partition operation failed: " + e);
    } finally {
      if(!success) {
        msdb.rollbackTransaction();
      }
    }

    return oldParts;
  }

  // Validate changes to partition's location to protect against errors on migration during
  // replication
  private void blockPartitionLocationChangesOnReplSource(Database db, Table tbl,
                                                         EnvironmentContext ec)
          throws InvalidOperationException {
    // If the database is not replication source, nothing to do
    if (!ReplChangeManager.isSourceOfReplication(db)) {
      return;
    }

    // Do not allow changing location of a managed table as alter event doesn't capture the
    // new files list. So, it may cause data inconsistency.
    if ((ec != null) && ec.isSetProperties()) {
      String alterType = ec.getProperties().get(ALTER_TABLE_OPERATION_TYPE);
      if (alterType != null && alterType.equalsIgnoreCase(ALTERLOCATION) &&
          tbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.name())) {
        String tableName = TableName.getQualified(tbl.getCatName(), tbl.getDbName(), tbl.getTableName());
        throw new InvalidOperationException(
            "Cannot change location of a managed table " + tableName + " as it is enabled for replication.");
      }
    }
  }

  // Validate changes to a table to protect against errors on migration during replication.
  public static void validateTableChangesOnReplSource(Configuration conf, Database db, Table oldTbl,
      Table newTbl, EnvironmentContext ec) throws InvalidOperationException {
    // If the database is not replication source, nothing to do
    if (!ReplChangeManager.isSourceOfReplication(db)) {
      return;
    }

    // Do not allow changing location of a managed table as alter event doesn't capture the
    // new files list. So, it may cause data inconsistency. We do this whether or not strict
    // managed is true on the source cluster.
    if ((ec != null) && ec.isSetProperties()) {
      String alterType = ec.getProperties().get(ALTER_TABLE_OPERATION_TYPE);
      if (alterType != null && alterType.equalsIgnoreCase(ALTERLOCATION) &&
          oldTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.name())) {
        String tableName = TableName.getQualified(oldTbl.getCatName(), oldTbl.getDbName(), oldTbl.getTableName());
        throw new InvalidOperationException(
            "Cannot change location of a managed table " + tableName + " as it is enabled for replication.");
        }
    }

    // Rest of the changes need validation only when strict managed tables is false. That's
    // when there's scope for migration during replication, at least for now.
    if (conf.getBoolean(MetastoreConf.ConfVars.STRICT_MANAGED_TABLES.getHiveName(), false)) {
      return;
    }

    // Do not allow changing the type of table. This is to avoid migration scenarios which causes
    // Managed ACID table to be converted to external at replica. As ACID tables cannot be
    // converted to external table and vice versa, we need to restrict this conversion at primary
    // as well. Currently, table type conversion is allowed only between managed and external
    // table types. But, to be future proof, any table type conversion is restricted on a
    // replication enabled DB.
    if (!oldTbl.getTableType().equalsIgnoreCase(newTbl.getTableType())) {
      throw new InvalidOperationException("Table type cannot be changed from " + oldTbl.getTableType()
              + " to " + newTbl.getTableType() + " for the table " +
              TableName.getQualified(oldTbl.getCatName(), oldTbl.getDbName(), oldTbl.getTableName())
              + " as it is enabled for replication.");
    }

    // Also we do not allow changing a non-Acid managed table to acid table on source with strict
    // managed false. After replicating a non-acid managed table to a target with strict managed
    // true the table will be converted to acid or external table. So changing the transactional
    // property of table on source can conflict with resultant change in the target.
    if (!TxnUtils.isTransactionalTable(oldTbl) && TxnUtils.isTransactionalTable(newTbl)) {
      throw new InvalidOperationException("A non-Acid table cannot be converted to an Acid " +
              "table for the table " + TableName.getQualified(oldTbl.getCatName(),
              oldTbl.getDbName(), oldTbl.getTableName()) + " as it is enabled for replication.");
    }
  }

  public static boolean checkPartialPartKeysEqual(List<FieldSchema> oldPartKeys,
      List<FieldSchema> newPartKeys) {
    //return true if both are null, or false if one is null and the other isn't
    if (newPartKeys == null || oldPartKeys == null) {
      return oldPartKeys == newPartKeys;
    }
    if (oldPartKeys.size() != newPartKeys.size()) {
      return false;
    }
    Iterator<FieldSchema> oldPartKeysIter = oldPartKeys.iterator();
    Iterator<FieldSchema> newPartKeysIter = newPartKeys.iterator();
    FieldSchema oldFs;
    FieldSchema newFs;
    while (oldPartKeysIter.hasNext()) {
      oldFs = oldPartKeysIter.next();
      newFs = newPartKeysIter.next();
      // Alter table can change the type of partition key now.
      // So check the column name only.
      if (!oldFs.getName().equals(newFs.getName())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Uses the scheme and authority of the object's current location and the path constructed
   * using the object's new name to construct a path for the object's new location.
   */
  public static Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
    URI currentUri = currentPath.toUri();

    return new Path(currentUri.getScheme(), currentUri.getAuthority(),
        defaultNewPath.toUri().getPath());
  }

  @VisibleForTesting
  public static void deleteTableColumnStats(RawStore msdb, Table oldTable, Table newTable)
      throws InvalidObjectException, MetaException {
    try {
      String catName = normalizeIdentifier(oldTable.isSetCatName()
          ? oldTable.getCatName()
          : getDefaultCatalog(msdb.getConf()));
      String dbName = oldTable.getDbName().toLowerCase();
      String tableName = normalizeIdentifier(oldTable.getTableName());
      List<String> staleColumns = findStaleColumns(oldTable.getSd().getCols(), newTable.getSd().getCols());
      if (!staleColumns.isEmpty() &&
          // Don't bother in the case of ACID conversion.
          TxnUtils.isAcidTable(oldTable) == TxnUtils.isAcidTable(newTable)) {
        msdb.deleteTableColumnStatistics(catName, dbName, tableName, staleColumns, null);
        Map<String, String> parameters = newTable.getParameters();
        if (parameters != null && parameters.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
          StatsSetupConst.removeColumnStatsState(parameters, staleColumns);
        }
      }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
    } catch (InvalidInputException e) {
      //should not happen since the input were verified before passed in
      throw new InvalidObjectException("Invalid inputs to update table column stats: " + e);
    }
  }

  public static void updateOrGetPartitionColumnStats(
      RawStore msdb, String catName, String dbname, String tblname, List<String> partVals,
      List<FieldSchema> oldCols, Table table, Partition part, List<String> deletedCols)
          throws MetaException, InvalidObjectException {
    try {
      List<FieldSchema> newCols  = part.getSd().getCols();
      String oldPartName = Warehouse.makePartName(table.getPartitionKeys(), partVals);
      // do not need to update column stats if existing columns haven't been changed
      List<String> staleColumns = findStaleColumns(oldCols, newCols);
      if (staleColumns.isEmpty()) {
        return;
      }
      if (deletedCols == null) {
        msdb.deletePartitionColumnStatistics(catName, dbname, tblname, Lists.newArrayList(oldPartName), staleColumns, null);
      } else {
        deletedCols.addAll(staleColumns);
      }
      Map<String, String> parameters = part.getParameters();
      if (parameters != null && parameters.containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
        StatsSetupConst.removeColumnStatsState(parameters, staleColumns);
      }
    } catch (NoSuchObjectException nsoe) {
      // ignore this exception, actually this exception won't be thrown from getPartitionColumnStatistics
    } catch (InvalidInputException iie) {
      throw new InvalidObjectException("Invalid input to delete partition column stats." + iie);
    }
  }
}
