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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.HiveAlterHandler;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreMetadataTransformer;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.DefaultIncompatibleTableChangeHandler;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@SuppressWarnings("unused")
@RequestHandler(requestBody = AlterTableRequest.class)
public class AlterTableHandler
    extends AbstractRequestHandler<AlterTableRequest, AlterTableHandler.AlterTableResult> {
  private static final Logger LOG = LoggerFactory.getLogger(AlterTableHandler.class);

  private String catName;
  private String dbname;
  private String name;
  private Table newTable;
  private EnvironmentContext envContext;
  private String validWriteIdList;
  private List<String> processorCapabilities;
  private String processorId;

  private RawStore msdb;
  private Warehouse wh;
  private Table oldTable;
  private boolean isReplicated;

  AlterTableHandler(IHMSHandler handler, AlterTableRequest request) {
    super(handler, false, request);
  }

  AlterTableHandler(IHMSHandler handler, DirectAlterContext ctx) {
    super(handler, false, new AlterTableRequest());
    this.msdb = ctx.msdb();
    this.wh = ctx.wh();
    this.catName = ctx.catName();
    this.dbname = ctx.dbname();
    this.name = ctx.name();
    this.newTable = ctx.newTable();
    this.envContext = ctx.envContext();
    this.validWriteIdList = ctx.validWriteIdList();
  }

  public static void runDirectAlter(IHMSHandler handler, DirectAlterContext ctx)
      throws InvalidOperationException, MetaException {
    try {
      AlterTableHandler op = new AlterTableHandler(handler, ctx);
      AlterTableResult result = op.alterTableCore();
      op.notifyRegularListeners(result);
    } catch (TException e) {
      throw new MetaException(e.getMessage());
    }
  }

  public record DirectAlterContext(RawStore msdb, Warehouse wh, String catName, String dbname, String name,
      Table newTable, EnvironmentContext envContext, String validWriteIdList) {
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.catName = request.isSetCatName() && request.getCatName() != null
        ? request.getCatName() : getDefaultCatalog(handler.getConf());
    this.dbname = request.getDbName();
    this.name = request.getTableName();
    this.newTable = request.getTable();
    this.validWriteIdList = request.getValidWriteIdList();
    this.processorCapabilities = request.getProcessorCapabilities();
    this.processorId = request.getProcessorIdentifier();

    this.envContext = request.getEnvironmentContext() != null
        ? request.getEnvironmentContext() : new EnvironmentContext();
    if (request.getExpectedParameterKey() != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY,
          request.getExpectedParameterKey());
    }
    if (request.getExpectedParameterValue() != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE,
          request.getExpectedParameterValue());
    }

    try {
      Database db = handler.get_database_core(catName, dbname);
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        throw new MetaException("Alter table in REMOTE database " + db.getName() + " is not allowed");
      }
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException("Alter table in REMOTE database is not allowed");
    }

    if (newTable.getParameters() == null
        || newTable.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
      newTable.putToParameters(hive_metastoreConstants.DDL_TIME,
          Long.toString(System.currentTimeMillis() / 1000));
    }

    if (newTable.getSd() != null) {
      String newLocation = newTable.getSd().getLocation();
      if (StringUtils.isNotEmpty(newLocation)) {
        Path tblPath = handler.getWh().getDnsPath(new Path(newLocation));
        newTable.getSd().setLocation(tblPath.toString());
      }
    }

    if (!newTable.isSetCatName()) {
      newTable.setCatName(catName);
    }

    GetTableRequest getReq = new GetTableRequest(dbname, name);
    getReq.setCatName(catName);
    Table oldt = handler.get_table_core(getReq);

    IMetaStoreMetadataTransformer transformer = handler.getMetadataTransformer();
    if (transformer != null) {
      newTable = transformer.transformAlterTable(oldt, newTable, processorCapabilities, processorId);
    }

    ((BaseHandler) handler).firePreEvent(new PreAlterTableEvent(oldt, newTable, handler));
  }

  @Override
  protected AlterTableResult execute() throws TException, IOException {
    this.msdb = handler.getMS();
    this.wh = handler.getWh();
    return alterTableCore();
  }

  private AlterTableResult alterTableCore() throws InvalidOperationException, MetaException {
    String catalogName = normalizeIdentifier(catName);
    String tableName = normalizeIdentifier(name);
    String databaseName = normalizeIdentifier(dbname);
    final boolean cascade;
    final boolean replDataLocationChanged;
    if ((envContext != null) && envContext.isSetProperties()) {
      cascade = StatsSetupConst.TRUE.equals(envContext.getProperties().get(StatsSetupConst.CASCADE));
      replDataLocationChanged = ReplConst.TRUE.equals(envContext.getProperties().get(ReplConst.REPL_DATA_LOCATION_CHANGED));
    } else {
      cascade = false;
      replDataLocationChanged = false;
    }
    if (newTable == null) {
      throw new InvalidOperationException("New table is null");
    }
    String newTblName = newTable.getTableName().toLowerCase();
    String newDbName = newTable.getDbName().toLowerCase();
    if (!MetaStoreUtils.validateName(newTblName, handler.getConf())) {
      throw new InvalidOperationException(newTblName + " is not a valid object name");
    }
    String validate = MetaStoreServerUtils.validateTblColumns(newTable.getSd().getCols());
    if (validate != null) {
      throw new InvalidOperationException("Invalid column " + validate);
    }
    // Validate bucketedColumns in new table
    List<String> bucketColumns = MetaStoreServerUtils.validateBucketColumns(newTable.getSd());
    if (CollectionUtils.isNotEmpty(bucketColumns)) {
      String errMsg = "Bucket columns - " + bucketColumns + " doesn't match with any table columns";
      LOG.error(errMsg);
      throw new InvalidOperationException(errMsg);
    }
    Path srcPath = null;
    FileSystem srcFs;
    Path destPath = null;
    FileSystem destFs = null;
    boolean success = false;
    boolean dataWasMoved = false;
    boolean isPartitionedTable = false;
    Database olddb = null;
    this.oldTable = null;
    List<TransactionalMetaStoreEventListener> transactionalListeners = handler.getTransactionalListeners();

    Map<String, String> txnAlterTableEventResponses = Collections.emptyMap();
    try {
      boolean rename = false;
      List<Partition> parts;
      // Switching tables between catalogs is not allowed.
      if (!catalogName.equalsIgnoreCase(newTable.getCatName())) {
        throw new InvalidOperationException("Tables cannot be moved between catalogs, old catalog" +
            catalogName + ", new catalog " + newTable.getCatName());
      }
      // check if table with the new name already exists
      if (!newTblName.equals(tableName) || !newDbName.equals(databaseName)) {
        if (msdb.getTable(catalogName, newDbName, newTblName, null) != null) {
          throw new InvalidOperationException("new table " + newDbName
              + "." + newTblName + " already exists");
        }
        rename = true;
      }
      String expectedKey = envContext != null && envContext.getProperties() != null ?
              envContext.getProperties().get(hive_metastoreConstants.EXPECTED_PARAMETER_KEY) : null;
      String expectedValue = envContext != null && envContext.getProperties() != null ?
              envContext.getProperties().get(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE) : null;
      msdb.openTransaction();
      // get old table
      // Note: we don't verify stats here; it's done below in alterTableUpdateTableColumnStats.
      olddb = msdb.getDatabase(catalogName, databaseName);
      this.oldTable = msdb.getTable(catalogName, databaseName, tableName, null);
      if (oldTable == null) {
        throw new InvalidOperationException("table " +
            TableName.getQualified(catalogName, databaseName, tableName) + " doesn't exist");
      }
      if (expectedKey != null && expectedValue != null) {
        String newValue = newTable.getParameters().get(expectedKey);
        if (newValue == null) {
          throw new MetaException(String.format("New value for expected key %s is not set", expectedKey));
        }
        if (!expectedValue.equals(oldTable.getParameters().get(expectedKey))) {
          throw new MetaException("The table has been modified. The parameter value for key '" + expectedKey + "' is '"
              + oldTable.getParameters().get(expectedKey) + "'. The expected was value was '" + expectedValue + "'");
        }
        long affectedRows = msdb.updateParameterWithExpectedValue(oldTable, expectedKey, expectedValue, newValue);
        if (affectedRows != 1) {
          // make sure concurrent modification exception messages have the same prefix
          throw new MetaException("The table has been modified. The parameter value for key '" + expectedKey + "' is different");
        }
      }
          HiveAlterHandler.validateTableChangesOnReplSource(handler.getConf(), olddb, oldTable, newTable,
              envContext);
      // On a replica this alter table will be executed only if old and new both the databases are
      // available and being replicated into. Otherwise, it will be either create or drop of table.
      this.isReplicated = isDbReplicationTarget(olddb);
      if (oldTable.getPartitionKeysSize() != 0) {
        isPartitionedTable = true;
      }
      // Throws InvalidOperationException if the new column types are not
      // compatible with the current column types.
      DefaultIncompatibleTableChangeHandler.get()
          .allowChange(handler.getConf(), oldTable, newTable);
      //check that partition keys have not changed, except for virtual views
      //however, allow the partition comments to change
          boolean partKeysPartiallyEqual = HiveAlterHandler.checkPartialPartKeysEqual(
              oldTable.getPartitionKeys(), newTable.getPartitionKeys());
      if (!oldTable.getTableType().equals(TableType.VIRTUAL_VIEW.toString())){
        Map<String, String> properties = envContext.getProperties();
        if (properties == null || !Boolean.parseBoolean(properties.getOrDefault(HiveMetaHook.ALLOW_PARTITION_KEY_CHANGE,
                "false"))) {
          if (!partKeysPartiallyEqual) {
            throw new InvalidOperationException("partition keys can not be changed.");
          }
        }
      }
      // Two mutually exclusive flows possible.
      // i) Partition locations needs update if replDataLocationChanged is true which means table's
      // data location is changed with all partition sub-directories.
      // ii) Rename needs change the data location and move the data to the new location corresponding
      // to the new name if:
      // 1) the table is not a virtual view, and
      // 2) the table is not an external table, and
      // 3) the user didn't change the default location (or new location is empty), and
      // 4) the table was not initially created with a specified location
      boolean renamedManagedTable = rename && !oldTable.getTableType().equals(TableType.VIRTUAL_VIEW.toString())
          && (oldTable.getSd().getLocation().compareTo(newTable.getSd().getLocation()) == 0
              || StringUtils.isEmpty(newTable.getSd().getLocation()))
          && (!MetaStoreUtils.isExternalTable(oldTable));
      Database db = msdb.getDatabase(catalogName, newDbName);
      boolean renamedTranslatedToExternalTable = rename && MetaStoreUtils.isTranslatedToExternalTable(oldTable)
          && MetaStoreUtils.isTranslatedToExternalTable(newTable);
      boolean renamedExternalTable = rename && MetaStoreUtils.isExternalTable(oldTable)
          && !MetaStoreUtils.isPropertyTrue(oldTable.getParameters(), HiveMetaHook.TRANSLATED_TO_EXTERNAL);
      boolean isRenameIcebergTable =
          rename && MetaStoreUtils.isIcebergTable(newTable.getParameters());
          HiveAlterHandler.deleteTableColumnStats(msdb, oldTable, newTable);
      if (!isRenameIcebergTable &&
          (replDataLocationChanged || renamedManagedTable || renamedTranslatedToExternalTable ||
              renamedExternalTable)) {
        srcPath = new Path(oldTable.getSd().getLocation());
        if (replDataLocationChanged) {
          // If data location is changed in replication flow, then new path was already set in
          // the newTable. Also, it is as good as the data is moved and set dataWasMoved=true so that
          // location in partitions are also updated accordingly.
          // No need to validate if the destPath exists as in replication flow, data gets replicated
          // separately.
          destPath = new Path(newTable.getSd().getLocation());
          dataWasMoved = true;
        } else if (!renamedExternalTable) {
          // Rename flow.
          // If a table was created in a user specified location using the DDL like
          // create table tbl ... location ...., it should be treated like an external table
          // in the table rename, its data location should not be changed. We can check
          // if the table directory was created directly under its database directory to tell
          // if it is such a table
          // Same applies to the ACID tables suffixed with the `txnId`, case with `lockless reads`.
          String oldtRelativePath = wh.getDatabaseManagedPath(olddb).toUri()
              .relativize(srcPath.toUri()).toString();
          boolean tableInSpecifiedLoc = !oldtRelativePath.equalsIgnoreCase(tableName)
                  && !oldtRelativePath.equalsIgnoreCase(tableName + Path.SEPARATOR);
          if (renamedTranslatedToExternalTable || !tableInSpecifiedLoc) {
            srcFs = wh.getFs(srcPath);
            // get new location
            assert(isReplicated == isDbReplicationTarget(db));
            if (renamedTranslatedToExternalTable) {
              if (!tableInSpecifiedLoc) {
                destPath = new Path(newTable.getSd().getLocation());
              } else {
                    Path databasePath = HiveAlterHandler.constructRenamedPath(
                        wh.getDatabaseExternalPath(db), srcPath);
                destPath = new Path(databasePath, newTblName);
                newTable.getSd().setLocation(destPath.toString());
              }
            } else {
                  Path databasePath = HiveAlterHandler.constructRenamedPath(
                      wh.getDatabaseManagedPath(db), srcPath);
              destPath = new Path(databasePath, newTblName);
              newTable.getSd().setLocation(destPath.toString());
            }
            destFs = wh.getFs(destPath);
            // check that destination does not exist otherwise we will be
            // overwriting data
            // check that src and dest are on the same file system
            if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
              throw new InvalidOperationException("table new location " + destPath
                      + " is on a different file system than the old location "
                      + srcPath + ". This operation is not supported");
            }
            try {
              if (destFs.exists(destPath)) {
                throw new InvalidOperationException("New location for this table " +
                        TableName.getQualified(catalogName, newDbName, newTblName) +
                        " already exists : " + destPath);
              }
              // check that src exists and also checks permissions necessary, rename src to dest
              if (srcFs.exists(srcPath) && wh.renameDir(srcPath, destPath,
                      ReplChangeManager.shouldEnableCm(olddb, oldTable))) {
                dataWasMoved = true;
              }
            } catch (IOException | MetaException e) {
              LOG.error("Alter Table operation for " + databaseName + "." + tableName + " failed.", e);
              throw new InvalidOperationException("Alter Table operation for " + databaseName + "." + tableName +
                          " failed to move data due to: '" + HiveAlterHandler.getSimpleMessage(e)
                      + "' See hive log file for details.");
            }
            if (!HiveMetaStore.isRenameAllowed(olddb, db)) {
              LOG.error("Alter Table operation for " + TableName.getQualified(catalogName, databaseName, tableName) +
                      "to new table = " + TableName.getQualified(catalogName, newDbName, newTblName) + " failed ");
              throw new MetaException("Alter table not allowed for table " +
                      TableName.getQualified(catalogName, databaseName, tableName) +
                      "to new table = " + TableName.getQualified(catalogName, newDbName, newTblName));
            }
          }
        }
        if (isPartitionedTable) {
          String oldTblLocPath = srcPath.toUri().getPath();
          String newTblLocPath = dataWasMoved ? destPath.toUri().getPath() : null;
          // Do not verify stats parameters on a partitioned table.
          msdb.alterTable(catalogName, databaseName, tableName, newTable, null);
          int partitionBatchSize = MetastoreConf.getIntVar(handler.getConf(),
              MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
          // alterPartition is only for changing the partition location in the table rename
          if (dataWasMoved) {
            PartitionsRequest req = new PartitionsRequest(newDbName, newTblName);
            req.setCatName(catName);
            req.setMaxParts((short) -1);
            parts = handler.get_partitions_req(req).getPartitions();
            for (Partition part : parts) {
              String oldPartLoc = part.getSd().getLocation();
              if (oldPartLoc.contains(oldTblLocPath)) {
                URI oldUri = new Path(oldPartLoc).toUri();
                String newPath = oldUri.getPath().replace(oldTblLocPath, newTblLocPath);
                Path newPartLocPath = new Path(oldUri.getScheme(), oldUri.getAuthority(), newPath);
                part.getSd().setLocation(newPartLocPath.toString());
              }
              part.setDbName(newDbName);
              part.setTableName(newTblName);
            }
            Batchable.runBatched(partitionBatchSize, parts, new Batchable<Partition, Void>() {
              @Override
              public List<Void> run(List<Partition> input) throws Exception {
                msdb.alterPartitions(catalogName, newDbName, newTblName,
                    input.stream().map(Partition::getValues).collect(Collectors.toList()),
                    input, newTable.getWriteId(), validWriteIdList);
                return Collections.emptyList();
              }
            });
          }
          Deadline.checkTimeout();
        } else {
          msdb.alterTable(catalogName, databaseName, tableName, newTable, validWriteIdList);
        }
      } else {
        // operations other than table rename
        if (MetaStoreServerUtils.requireCalStats(null, null, newTable, envContext) &&
            !isPartitionedTable) {
          assert(isReplicated == isDbReplicationTarget(db));
          // Update table stats. For partitioned table, we update stats in alterPartition()
          MetaStoreServerUtils.updateTableStatsSlow(db, newTable, wh, false, true, envContext);
        }
        if (isPartitionedTable) {
          //Currently only column related changes can be cascaded in alter table
          boolean runPartitionMetadataUpdate =
              (cascade && !MetaStoreServerUtils.areSameColumns(oldTable.getSd().getCols(), newTable.getSd().getCols()));
          // we may skip the update entirely if there are only new columns added
          runPartitionMetadataUpdate |=
              !cascade && !MetaStoreServerUtils.arePrefixColumns(oldTable.getSd().getCols(), newTable.getSd().getCols());
          boolean retainOnColRemoval =
              MetastoreConf.getBoolVar(handler.getConf(), MetastoreConf.ConfVars.COLSTATS_RETAIN_ON_COLUMN_REMOVAL);
          if (runPartitionMetadataUpdate) {
            // Don't validate table-level stats for a partitoned table.
            msdb.alterTable(catalogName, databaseName, tableName, newTable, null);
            if (cascade || retainOnColRemoval) {
              PartitionsRequest req = new PartitionsRequest(dbname, name);
              req.setCatName(catName);
              req.setMaxParts((short) -1);
              parts = handler.get_partitions_req(req).getPartitions();
              Table table = oldTable;
              int partitionBatchSize = MetastoreConf.getIntVar(handler.getConf(),
                  MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
              Map<List<String>, List<List<String>>> changedColsToPartNames = new HashMap<>();
              Batchable.runBatched(partitionBatchSize, parts, new Batchable<Partition, Void>() {
                @Override
                public List<Void> run(List<Partition> input) throws Exception {
                  List<Partition> oldParts = new ArrayList<>(input.size());
                  List<List<String>> partVals = input.stream().map(Partition::getValues).collect(Collectors.toList());
                  for (Partition part : input) {
                    Partition oldPart = new Partition(part);
                    List<FieldSchema> oldCols = part.getSd().getCols();
                    part.getSd().setCols(newTable.getSd().getCols());
                    List<String> deletedCols = new ArrayList<>();
                    HiveAlterHandler.updateOrGetPartitionColumnStats(msdb, catalogName, databaseName,
                        tableName, part.getValues(), oldCols, table, part, deletedCols);
                    if (!deletedCols.isEmpty()) {
                      changedColsToPartNames.compute(deletedCols, (k, v) -> {
                        if (v == null) v = new ArrayList<>();
                        v.add(part.getValues());
                        return v;
                      });
                    }
                    if (!cascade) {
                      // update changed properties (stats)
                      oldPart.setParameters(part.getParameters());
                      oldParts.add(oldPart);
                    }
                  }
                  Deadline.checkTimeout();
                  msdb.alterPartitions(catalogName, databaseName, tableName,
                      partVals, (cascade) ? input : oldParts, newTable.getWriteId(), validWriteIdList);
                  return Collections.emptyList();
                }
              });
              for (Map.Entry<List<String>, List<List<String>>> entry : changedColsToPartNames.entrySet()) {
                List<String> partNames = new ArrayList<>();
                for (List<String> part_vals : entry.getValue()) {
                  partNames.add(Warehouse.makePartName(table.getPartitionKeys(), part_vals));
                }
                msdb.deletePartitionColumnStatistics(catalogName, databaseName, tableName, partNames, entry.getKey(), null);
              }
            } else {
              // clear all column stats to prevent incorract behaviour in case same column is reintroduced
              msdb.deleteAllPartitionColumnStatistics(
                  new TableName(catalogName, databaseName, tableName), validWriteIdList);
            }
          } else {
            LOG.warn("Alter table not cascaded to partitions.");
            msdb.alterTable(catalogName, databaseName, tableName, newTable, validWriteIdList);
          }
        } else {
          msdb.alterTable(catalogName, databaseName, tableName, newTable, validWriteIdList);
        }
      }
      if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
        txnAlterTableEventResponses = MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventMessage.EventType.ALTER_TABLE,
                  new AlterTableEvent(oldTable, newTable, false, true,
                          newTable.getWriteId(), handler, isReplicated),
                  envContext);
      }
      // commit the changes
      success = msdb.commitTransaction();
    } catch (InvalidOperationException | MetaException e) {
      throw e;
    } catch (TException e) {
      LOG.debug("Failed to get object from Metastore ", e);
      throw new InvalidOperationException(
          "Unable to change partition or table."
              + " Check metastore logs for detailed stack." + e.getMessage());
    } finally {
      if (success) {
        // Txn was committed successfully.
        // If data location is changed in replication flow, then need to delete the old path.
        if (replDataLocationChanged) {
          Path deleteOldDataLoc = new Path(oldTable.getSd().getLocation());
          boolean isSkipTrash = MetaStoreUtils.isSkipTrash(oldTable.getParameters());
          try {
            wh.deleteDir(deleteOldDataLoc, isSkipTrash,
                    ReplChangeManager.shouldEnableCm(olddb, oldTable));
            LOG.info("Deleted the old data location: {} for the table: {}",
                    deleteOldDataLoc, databaseName + "." + tableName);
          } catch (MetaException ex) {
            // Eat the exception as it doesn't affect the state of existing tables.
            // Expect, user to manually drop this path when exception and so logging a warning.
            LOG.warn("Unable to delete the old data location: {} for the table: {}",
                    deleteOldDataLoc, databaseName + "." + tableName);
          }
        }
      } else {
        LOG.error("Failed to alter table " + TableName.getQualified(catalogName, databaseName, tableName));
        msdb.rollbackTransaction();
        if (!replDataLocationChanged && dataWasMoved) {
          try {
            if (destFs.exists(destPath)) {
              if (!destFs.rename(destPath, srcPath)) {
                LOG.error("Failed to restore data from " + destPath + " to " + srcPath
                    + " in alter table failure. Manual restore is needed.");
              }
            }
          } catch (IOException e) {
            LOG.error("Failed to restore data from " + destPath + " to " + srcPath
                +  " in alter table failure. Manual restore is needed.");
          }
        }
      }
    }
    return new AlterTableResult(success, txnAlterTableEventResponses);
  }

  private void notifyRegularListeners(AlterTableResult result) throws MetaException, TException {
    if (result != null && !handler.getListeners().isEmpty() && newTable != null) {
      boolean altered = result.success() && oldTable != null;
      MetaStoreListenerNotifier.notifyEvent(handler.getListeners(), EventMessage.EventType.ALTER_TABLE,
          new AlterTableEvent(oldTable, newTable, false, altered,
              newTable.getWriteId(), handler, isReplicated),
          envContext, result.transactionalListenerResponses(), msdb);
    }
  }

  @Override
  protected void afterExecute(AlterTableResult result) throws TException, IOException {
    notifyRegularListeners(result);
    super.afterExecute(result);
  }

  @Override
  public String toString() {
    return "AlterTableHandler [" + id + "] - alter table "
        + TableName.getQualified(catName, dbname, name) + ":";
  }

  public record AlterTableResult(boolean success, Map<String, String> transactionalListenerResponses)
      implements Result {
  }
}
