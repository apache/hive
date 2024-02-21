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

package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.drop.DropTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.add.AlterTableAddPartitionDesc;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.DeferredWorkContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * ImportSemanticAnalyzer.
 *
 */
public class ImportSemanticAnalyzer extends BaseSemanticAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(ImportSemanticAnalyzer.class);

  public ImportSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  // Note that the tableExists flag as used by Auth is kinda a hack and
  // assumes only 1 table will ever be imported - this assumption is broken by
  // REPL LOAD.
  //
  // However, we've not chosen to expand this to a map of tables/etc, since
  // we have expanded how auth works with REPL DUMP / REPL LOAD to simply
  // require ADMIN privileges, rather than checking each object, which
  // quickly becomes untenable, and even more so, costly on memory.
  private boolean tableExists = false;

  public boolean existsTable() {
    return tableExists;
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    try {
      Tree fromTree = ast.getChild(0);

      boolean isLocationSet = false;
      boolean isExternalSet = false;
      boolean isPartSpecSet = false;
      String parsedLocation = null;
      String parsedTableName = null;
      String parsedDbName = null;
      LinkedHashMap<String, String> parsedPartSpec = new LinkedHashMap<String, String>();

      // waitOnPrecursor determines whether or not non-existence of
      // a dependent object is an error. For regular imports, it is.
      // for now, the only thing this affects is whether or not the
      // db exists.
      boolean waitOnPrecursor = false;

      for (int i = 1; i < ast.getChildCount(); ++i) {
        ASTNode child = (ASTNode) ast.getChild(i);
        switch (child.getToken().getType()) {
          case HiveParser.KW_EXTERNAL:
            isExternalSet = true;
            break;
          case HiveParser.TOK_TABLELOCATION:
            isLocationSet = true;
            parsedLocation = EximUtil.relativeToAbsolutePath(conf, unescapeSQLString(child.getChild(0).getText()));
            break;
          case HiveParser.TOK_TAB:
            ASTNode tableNameNode = (ASTNode) child.getChild(0);
            Map.Entry<String, String> dbTablePair = getDbTableNamePair(tableNameNode);
            parsedDbName = dbTablePair.getKey();
            parsedTableName = dbTablePair.getValue();
            // get partition metadata if partition specified
            if (child.getChildCount() == 2) {
              @SuppressWarnings("unused")
              ASTNode partspec = (ASTNode) child.getChild(1);
              isPartSpecSet = true;
              parsePartitionSpec(child, parsedPartSpec);
            }
            break;
        }
      }

      if (StringUtils.isEmpty(parsedDbName)) {
        parsedDbName = SessionState.get().getCurrentDatabase();
      }
      // parsing statement is now done, on to logic.
      EximUtil.SemanticAnalyzerWrapperContext x =
          new EximUtil.SemanticAnalyzerWrapperContext(conf, db, inputs, outputs, rootTasks, LOG, ctx);
      MetaData rv = EximUtil.getMetaDataFromLocation(fromTree.getText(), x.getConf());
      tableExists = prepareImport(true,
              isLocationSet, isExternalSet, isPartSpecSet, waitOnPrecursor,
              parsedLocation, parsedTableName, parsedDbName, parsedPartSpec, fromTree.getText(),
              x, null, getTxnMgr(), 0, rv);

    } catch (SemanticException e) {
      throw e;
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.IMPORT_SEMANTIC_ERROR.getMsg(), e);
    }
  }

  private void parsePartitionSpec(ASTNode tableNode, LinkedHashMap<String, String> partSpec) throws SemanticException {
    // get partition metadata if partition specified
    if (tableNode.getChildCount() == 2) {
      ASTNode partspec = (ASTNode) tableNode.getChild(1);
      // partSpec is a mapping from partition column name to its value.
      for (int j = 0; j < partspec.getChildCount(); ++j) {
        ASTNode partspec_val = (ASTNode) partspec.getChild(j);
        String val = null;
        String colName = unescapeIdentifier(partspec_val.getChild(0)
                .getText().toLowerCase());
        if (partspec_val.getChildCount() < 2) { // DP in the form of T
          // partition (ds, hr)
          throw new SemanticException(
                  ErrorMsg.INVALID_PARTITION
                          .getMsg(" - Dynamic partitions not allowed"));
        } else { // in the form of T partition (ds="2010-03-03")
          val = stripQuotes(partspec_val.getChild(1).getText());
        }
        partSpec.put(colName, val);
      }
    }
  }

  /**
   * The same code is used from both the "repl load" as well as "import".
   * Given that "repl load" now supports two modes "repl load dbName [location]" and
   * "repl load [location]" in which case the database name has to be taken from the table metadata
   * by default and then over-ridden if something specified on the command line.
   * <p>
   * hence for import to work correctly we have to pass in the sessionState default Db via the
   * parsedDbName parameter
   */
  public static boolean prepareImport(boolean isImportCmd,
                                      boolean isLocationSet, boolean isExternalSet, boolean isPartSpecSet,
                                      boolean waitOnPrecursor,
                                      String parsedLocation, String parsedTableName, String overrideDBName,
                                      LinkedHashMap<String, String> parsedPartSpec,
                                      String fromLocn, EximUtil.SemanticAnalyzerWrapperContext x,
                                      UpdatedMetaDataTracker updatedMetadata, HiveTxnManager txnMgr,
                                      long writeId, // Initialize with 0 for non-ACID and non-MM tables.
                                      MetaData rv
  ) throws IOException, MetaException, HiveException, URISyntaxException {
    return prepareImport(isImportCmd, isLocationSet, isExternalSet, isPartSpecSet, waitOnPrecursor,
                         parsedLocation, parsedTableName, overrideDBName, parsedPartSpec, fromLocn,
                         x, updatedMetadata, txnMgr, writeId, rv, null, null);
  }

  public static boolean prepareImport(boolean isImportCmd,
                                      boolean isLocationSet, boolean isExternalSet, boolean isPartSpecSet,
                                      boolean waitOnPrecursor,
                                      String parsedLocation, String parsedTableName, String overrideDBName,
                                      LinkedHashMap<String, String> parsedPartSpec,
                                      String fromLocn, EximUtil.SemanticAnalyzerWrapperContext x,
                                      UpdatedMetaDataTracker updatedMetadata, HiveTxnManager txnMgr,
                                      long writeId, // Initialize with 0 for non-ACID and non-MM tables.
                                      MetaData rv,
                                      String dumpRoot,
                                      ReplicationMetricCollector metricCollector
  ) throws IOException, MetaException, HiveException, URISyntaxException {

    // initialize load path
    URI fromURI = EximUtil.getValidatedURI(x.getConf(), stripQuotes(fromLocn));
    Path fromPath = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());

    FileSystem fs = FileSystem.get(fromURI, x.getConf());
    x.getInputs().add(toReadEntity(fromPath, x.getConf()));

    if (rv.getTable() == null) {
      // nothing to do here, silently return.
      return false;
    }

    ReplicationSpec replicationSpec = rv.getReplicationSpec();
    if (replicationSpec.isNoop()) {
      // nothing to do here, silently return.
      x.getLOG().debug("Current update with ID:{} is noop",
              replicationSpec.getCurrentReplicationState());
      return false;
    }

    if (isImportCmd) {
      replicationSpec.setReplSpecType(ReplicationSpec.Type.IMPORT);
    }

    String dbname = rv.getTable().getDbName();
    if ((overrideDBName != null) && (!overrideDBName.isEmpty())) {
      // If the parsed statement contained a db.tablename specification, prefer that.
      dbname = overrideDBName;
    }

    // Create table associated with the import
    // Executed if relevant, and used to contain all the other details about the table if not.
    ImportTableDesc tblDesc;
    org.apache.hadoop.hive.metastore.api.Table tblObj = rv.getTable();
    try {
      tblDesc = getBaseCreateTableDescFromTable(dbname, tblObj);
    } catch (Exception e) {
      throw new HiveException(e);
    }

    boolean inReplicationScope = false;
    if ((replicationSpec != null) && replicationSpec.isInReplicationScope()) {
      tblDesc.setReplicationSpec(replicationSpec);
      inReplicationScope = true;
      tblDesc.setReplWriteId(writeId);
      tblDesc.setOwnerName(tblObj.getOwner());
    }

    if (isExternalSet) {
      tblDesc.setExternal(isExternalSet);
      // This condition-check could have been avoided, but to honour the old
      // default of not calling if it wasn't set, we retain that behaviour.
      // TODO:cleanup after verification that the outer if isn't really needed here
    }

    if (isLocationSet) {
      STATIC_LOG.debug("table {} location is {}", tblDesc.getTableName(), parsedLocation);
      tblDesc.setLocation(parsedLocation);
      x.getInputs().add(toReadEntity(new Path(parsedLocation), x.getConf()));
    }

    if (StringUtils.isNotBlank(parsedTableName)) {
      tblDesc.setTableName(TableName.fromString(parsedTableName, null, dbname));
    }

    if (tblDesc.getTableName() == null) {
      // Either we got the tablename from the IMPORT statement (first priority) or from the export dump.
      throw new SemanticException(ErrorMsg.NEED_TABLE_SPECIFICATION.getMsg());
    } else {
      x.getConf().set("import.destination.table", tblDesc.getTableName());
    }

    List<AlterTableAddPartitionDesc> partitionDescs = new ArrayList<>();
    Iterable<Partition> partitions = rv.getPartitions();
    for (Partition partition : partitions) {
      // TODO: this should ideally not create AddPartitionDesc per partition
      AlterTableAddPartitionDesc partsDesc =
              getBaseAddPartitionDescFromPartition(fromPath, dbname, tblDesc, partition,
                      replicationSpec, x.getConf());
      partitionDescs.add(partsDesc);
    }

    if (isPartSpecSet) {
      // The import specification asked for only a particular partition to be loaded
      // We load only that, and ignore all the others.
      boolean found = false;
      for (Iterator<AlterTableAddPartitionDesc> partnIter = partitionDescs
              .listIterator(); partnIter.hasNext(); ) {
        AlterTableAddPartitionDesc addPartitionDesc = partnIter.next();
        if (!found && addPartitionDesc.getPartitions().get(0).getPartSpec().equals(parsedPartSpec)) {
          found = true;
        } else {
          partnIter.remove();
        }
      }
      if (!found) {
        throw new SemanticException(
                ErrorMsg.INVALID_PARTITION
                        .getMsg(" - Specified partition not found in import directory"));
      }
    }

    Warehouse wh = new Warehouse(x.getConf());
    Table table = tableIfExists(tblDesc, x.getHive());
    boolean tableExists = false;

    if (table != null) {
      checkTable(table, tblDesc, replicationSpec, x.getConf());
      x.getLOG().debug("table " + tblDesc.getTableName() + " exists: metadata checked");
      tableExists = true;
    }

    if (!tableExists && isExternalSet) {
      // If the user is explicitly importing a new external table, clear txn flags from the spec.
      AcidUtils.setNonTransactional(tblDesc.getTblProps());
    }

    int stmtId = 0;
    if (!replicationSpec.isInReplicationScope()
            && ((tableExists && AcidUtils.isTransactionalTable(table))
            || (!tableExists && AcidUtils.isTablePropertyTransactional(tblDesc.getTblProps())))) {
      //if importing into existing transactional table or will create a new transactional table
      //(because Export was done from transactional table), need a writeId
      // Explain plan doesn't open a txn and hence no need to allocate write id.
      // In replication flow, no need to allocate write id. It will be allocated using the alloc write id event.
      if (x.getCtx().getExplainConfig() == null && !inReplicationScope) {
        writeId = txnMgr.getTableWriteId(tblDesc.getDatabaseName(), tblDesc.getTableName());
        stmtId = txnMgr.getStmtIdAndIncrement();
      }
    }

    if (inReplicationScope) {
      createReplImportTasks(
              tblDesc, partitionDescs,
              replicationSpec, waitOnPrecursor, table,
              fromURI, wh, x, writeId, stmtId, updatedMetadata, dumpRoot, metricCollector);
    } else {
      createRegularImportTasks(
              tblDesc, partitionDescs,
              isPartSpecSet, replicationSpec, table,
              fromURI, fs, wh, x, writeId, stmtId);
    }
    return tableExists;
  }

  private static AlterTableAddPartitionDesc getBaseAddPartitionDescFromPartition(Path fromPath, String dbName,
      ImportTableDesc tblDesc, Partition partition, ReplicationSpec replicationSpec, HiveConf conf)
          throws MetaException, SemanticException {
    Map<String, String> partitionSpec = EximUtil.makePartSpec(tblDesc.getPartCols(), partition.getValues());

    StorageDescriptor sd = partition.getSd();

    String location = null;
    if (replicationSpec.isInReplicationScope() && tblDesc.isExternal()) {
      location = ReplExternalTables.externalTableLocation(conf, partition.getSd().getLocation());
      LOG.debug("partition {} has data location: {}", partition, location);
    } else {
      location = new Path(fromPath, Warehouse.makePartName(tblDesc.getPartCols(), partition.getValues())).toString();
    }

    long writeId = -1;
    if (tblDesc.getReplWriteId() != null) {
      writeId = tblDesc.getReplWriteId();
    }

    AlterTableAddPartitionDesc.PartitionDesc partitionDesc = new AlterTableAddPartitionDesc.PartitionDesc(
        partitionSpec, location, partition.getParameters(), sd.getInputFormat(), sd.getOutputFormat(),
        sd.getNumBuckets(), sd.getCols(), sd.getSerdeInfo().getSerializationLib(), sd.getSerdeInfo().getParameters(),
        sd.getBucketCols(), sd.getSortCols(), null, writeId);
    return new AlterTableAddPartitionDesc(dbName, tblDesc.getTableName(), true, ImmutableList.of(partitionDesc));
  }

  private static ImportTableDesc getBaseCreateTableDescFromTable(String dbName,
                                                                 org.apache.hadoop.hive.metastore.api.Table tblObj)
          throws Exception {
    Table table = new Table(tblObj);
    return new ImportTableDesc(dbName, table);
  }

  private static Task<?> loadTable(URI fromURI, ImportTableDesc tblDesc, boolean replace, Path tgtPath,
                                   ReplicationSpec replicationSpec, EximUtil.SemanticAnalyzerWrapperContext x,
                                   Long writeId, int stmtId) throws HiveException {
    return loadTable(fromURI, tblDesc, replace, tgtPath, replicationSpec, x, writeId,stmtId, null, null);
  }

  /*
   * This API reads the table metadata and updates the deferred work context object.
   */
  public static void setupDeferredContextFromMetadata(DeferredWorkContext deferredContext) throws HiveException {

    deferredContext.table = ImportSemanticAnalyzer.tableIfExists(deferredContext.tblDesc, deferredContext.hive);
    if (deferredContext.table == null) {
      deferredContext.table = ImportSemanticAnalyzer.createNewTableMetadataObject(deferredContext.tblDesc, true);
    }

    if (deferredContext.inReplScope) {
      deferredContext.isSkipTrash = MetaStoreUtils.isSkipTrash(deferredContext.table.getParameters());
      if (deferredContext.table.isTemporary()) {
        deferredContext.needRecycle = false;
      } else {
        org.apache.hadoop.hive.metastore.api.Database db = deferredContext.hive.getDatabase(deferredContext.table.getDbName());
        deferredContext.needRecycle = db != null && ReplChangeManager.shouldEnableCm(db, deferredContext.table.getTTable());
      }
    }

    if (AcidUtils.isTransactionalTable(deferredContext.table)) {
      String mmSubdir = deferredContext.replace ? AcidUtils.baseDir(deferredContext.writeId) :
              AcidUtils.deltaSubdir(deferredContext.writeId, deferredContext.writeId, deferredContext.stmtId);
      deferredContext.destPath = new Path(deferredContext.tgtPath, mmSubdir);
      /*
         CopyTask will copy files from the 'archive' to a delta_x_x in the table/partition
         directory, i.e. the final destination for these files.  This has to be a copy to preserve
         the archive.  MoveTask is optimized to do a 'rename' if files are on the same FileSystem.
         So setting 'loadPath' this way will make
         {@link Hive#loadTable(Path, String, LoadTableDesc.LoadFileType, boolean, boolean, boolean,
         boolean, Long, int, boolean, boolean)}
          skip the unnecessary file (rename) operation but it will perform other things.
      */
      deferredContext.loadPath = deferredContext.tgtPath;
      deferredContext.loadFileType = LoadTableDesc.LoadFileType.KEEP_EXISTING;
    } else {
      deferredContext.destPath = deferredContext.loadPath = deferredContext.ctx.getExternalTmpPath(deferredContext.tgtPath);
      deferredContext.loadFileType = deferredContext.replace ? LoadTableDesc.LoadFileType.REPLACE_ALL : LoadTableDesc.LoadFileType.OVERWRITE_EXISTING;
    }
    deferredContext.isCalculated = true;
  }

  private static Task<?> loadTable(URI fromURI, ImportTableDesc tblDesc, boolean replace, Path tgtPath,
                                   ReplicationSpec replicationSpec, EximUtil.SemanticAnalyzerWrapperContext x,
                                   Long writeId, int stmtId,
                                   String dumpRoot, ReplicationMetricCollector metricCollector) throws HiveException {


    Path dataPath = new Path(fromURI.toString(), EximUtil.DATA_PATH_NAME);

    DeferredWorkContext
        resolver = new DeferredWorkContext(replace, tgtPath, writeId, stmtId, x.getHive(), x.getCtx(), tblDesc,
            replicationSpec.isInReplicationScope());

    Task<?> copyTask;
    // Corresponding work instances are not complete yet. Some of the values will be calculated and assigned when task
    // is being executed.
    if (replicationSpec.isInReplicationScope()) {
      boolean copyAtLoad = x.getConf().getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
      copyTask = ReplCopyTask.getLoadCopyTask(replicationSpec, dataPath, null, x.getConf(),
              false, false, copyAtLoad, dumpRoot, metricCollector);
    } else {
      copyTask = TaskFactory.get(new CopyWork(dataPath, null, false, dumpRoot, metricCollector, true));
    }

    copyTask.setDeferredWorkContext(resolver);

    MoveWork moveWork = new MoveWork(x.getInputs(), x.getOutputs(), null, null, false,
        dumpRoot, metricCollector, true);

    //if Importing into existing table, FileFormat is checked by
    // ImportSemanticAnalyzer.checked checkTable()
    Task<?> loadTableTask = TaskFactory.get(moveWork, x.getConf());
    loadTableTask.setDeferredWorkContext(resolver);
    copyTask.addDependentTask(loadTableTask);
    x.getTasks().add(copyTask);
    return loadTableTask;
  }

  private static Task<?> createTableTask(ImportTableDesc tableDesc, EximUtil.SemanticAnalyzerWrapperContext x) {
    return tableDesc.getCreateTableTask(x.getInputs(), x.getOutputs(), x.getConf());
  }

  private static Task<?> createTableTask(ImportTableDesc tableDesc, EximUtil.SemanticAnalyzerWrapperContext x,
                                         String dumpRoot, ReplicationMetricCollector metricCollector) {
    return tableDesc.getCreateTableTask(x.getInputs(), x.getOutputs(), x.getConf(), true,
                                        dumpRoot, metricCollector, false);
  }

  private static Task<?> dropTableTask(Table table, EximUtil.SemanticAnalyzerWrapperContext x,
                                       ReplicationSpec replicationSpec) {
    DropTableDesc dropTblDesc = new DropTableDesc(table.getTableName(), true, false, replicationSpec);
    return TaskFactory.get(new DDLWork(x.getInputs(), x.getOutputs(), dropTblDesc), x.getConf());
  }

  private static Task<?> dropTableTask(Table table, EximUtil.SemanticAnalyzerWrapperContext x,
                                       ReplicationSpec replicationSpec, String dumpRoot,
                                       ReplicationMetricCollector metricCollector) {
    DropTableDesc dropTblDesc = new DropTableDesc(table.getTableName(), true, false, replicationSpec);
    return TaskFactory.get(new DDLWork(x.getInputs(), x.getOutputs(), dropTblDesc,
            true, dumpRoot, metricCollector), x.getConf());
  }

  private static Task<?> alterTableTask(ImportTableDesc tableDesc,
                                                             EximUtil.SemanticAnalyzerWrapperContext x,
                                                             ReplicationSpec replicationSpec) {
    tableDesc.setReplaceMode(true);
    if ((replicationSpec != null) && (replicationSpec.isInReplicationScope())) {
      tableDesc.setReplicationSpec(replicationSpec);
    }
    return tableDesc.getCreateTableTask(x.getInputs(), x.getOutputs(), x.getConf());
  }

  private static Task<?> alterTableTask(ImportTableDesc tableDesc,
                                        EximUtil.SemanticAnalyzerWrapperContext x,
                                        ReplicationSpec replicationSpec, boolean isReplication,
                                        String dumpRoot, ReplicationMetricCollector metricCollector) {
    tableDesc.setReplaceMode(true);
    if ((replicationSpec != null) && (replicationSpec.isInReplicationScope())) {
      tableDesc.setReplicationSpec(replicationSpec);
    }
    return tableDesc.getCreateTableTask(x.getInputs(), x.getOutputs(), x.getConf(), isReplication,
                                        dumpRoot, metricCollector, false);
  }

  private static Task<?> alterSinglePartition(
          ImportTableDesc tblDesc, Table table, Warehouse wh, AlterTableAddPartitionDesc addPartitionDesc,
          ReplicationSpec replicationSpec, org.apache.hadoop.hive.ql.metadata.Partition ptn,
          EximUtil.SemanticAnalyzerWrapperContext x) throws MetaException, IOException, HiveException {
    if ((replicationSpec != null) && (replicationSpec.isInReplicationScope())) {
      addPartitionDesc.setReplicationSpec(replicationSpec);
    }
    AlterTableAddPartitionDesc.PartitionDesc partSpec = addPartitionDesc.getPartitions().get(0);
    if (ptn == null) {
      fixLocationInPartSpec(tblDesc, table, wh, replicationSpec, partSpec, x);
    } else if (!externalTablePartition(tblDesc, replicationSpec)) {
      partSpec.setLocation(ptn.getLocation()); // use existing location
    }
    return TaskFactory.get(new DDLWork(x.getInputs(), x.getOutputs(), addPartitionDesc), x.getConf());
  }

  private static Task<?> alterSinglePartition(
          ImportTableDesc tblDesc, Table table, Warehouse wh, AlterTableAddPartitionDesc addPartitionDesc,
          ReplicationSpec replicationSpec, org.apache.hadoop.hive.ql.metadata.Partition ptn,
          EximUtil.SemanticAnalyzerWrapperContext x, boolean isReplication,
          String dumpRoot, ReplicationMetricCollector metricCollector) throws MetaException, IOException, HiveException {
    if ((replicationSpec != null) && (replicationSpec.isInReplicationScope())) {
      addPartitionDesc.setReplicationSpec(replicationSpec);
    }
    AlterTableAddPartitionDesc.PartitionDesc partSpec = addPartitionDesc.getPartitions().get(0);
    if (ptn == null) {
      fixLocationInPartSpec(tblDesc, table, wh, replicationSpec, partSpec, x);
    } else if (!externalTablePartition(tblDesc, replicationSpec)) {
      partSpec.setLocation(ptn.getLocation()); // use existing location
    }
    return TaskFactory.get(new DDLWork(x.getInputs(), x.getOutputs(), addPartitionDesc,
            isReplication, dumpRoot, metricCollector), x.getConf());
  }

  private static Task<?> addSinglePartition(ImportTableDesc tblDesc,
                                            Table table, Warehouse wh, AlterTableAddPartitionDesc addPartitionDesc,
                                            ReplicationSpec replicationSpec,
                                            EximUtil.SemanticAnalyzerWrapperContext x, Long writeId, int stmtId)
          throws MetaException, IOException, HiveException {
    return addSinglePartition(tblDesc, table, wh, addPartitionDesc, replicationSpec,
                              x, writeId, stmtId, false, null, null);
  }

    private static Task<?> addSinglePartition(ImportTableDesc tblDesc,
                                            Table table, Warehouse wh, AlterTableAddPartitionDesc addPartitionDesc,
                                            ReplicationSpec replicationSpec,
                                            EximUtil.SemanticAnalyzerWrapperContext x, Long writeId, int stmtId,
                                            boolean isReplication, String dumpRoot,
                                            ReplicationMetricCollector metricCollector)
          throws MetaException, IOException, HiveException {
    AlterTableAddPartitionDesc.PartitionDesc partSpec = addPartitionDesc.getPartitions().get(0);
    boolean isSkipTrash = false;
    boolean needRecycle = false;

    if (shouldSkipDataCopyInReplScope(tblDesc, replicationSpec)
            || (tblDesc.isExternal() && tblDesc.getLocation() == null)) {
      x.getLOG().debug("Adding AddPart and skipped data copy for partition "
              + partSpecToString(partSpec.getPartSpec()));
      // addPartitionDesc already has the right partition location
      @SuppressWarnings("unchecked")
      Task<?> addPartTask = TaskFactory.get(
              new DDLWork(x.getInputs(), x.getOutputs(), addPartitionDesc, isReplication,
                      dumpRoot, metricCollector), x.getConf());
      return addPartTask;
    } else {
      String srcLocation = partSpec.getLocation();
      if (replicationSpec.isInReplicationScope()
          && !ReplicationSpec.Type.IMPORT.equals(replicationSpec.getReplSpecType())) {
        Path partLocation = new Path(partSpec.getLocation());
        Path dataDirBase = partLocation.getParent();
        String bucketDir = partLocation.getName();
        for (int i=1; i<partSpec.getPartSpec().size(); i++) {
          bucketDir =  dataDirBase.getName() + File.separator + bucketDir;
          dataDirBase = dataDirBase.getParent();
        }
        String relativePartDataPath = EximUtil.DATA_PATH_NAME + File.separator + bucketDir;
        srcLocation =  new Path(dataDirBase, relativePartDataPath).toString();
      }
      fixLocationInPartSpec(tblDesc, table, wh, replicationSpec, partSpec, x);
      x.getLOG().debug("adding dependent CopyWork/AddPart/MoveWork for partition "
              + partSpecToString(partSpec.getPartSpec())
              + " with source location: " + srcLocation);
      Path tgtLocation = new Path(partSpec.getLocation());

      LoadFileType loadFileType;
      Path destPath;
      if (replicationSpec.isInReplicationScope()) {
        loadFileType = LoadFileType.IGNORE;
        destPath = tgtLocation;
        isSkipTrash = MetaStoreUtils.isSkipTrash(table.getParameters());
        if (table.isTemporary()) {
          needRecycle = false;
        } else {
          org.apache.hadoop.hive.metastore.api.Database db = x.getHive().getDatabase(table.getDbName());
          needRecycle = db != null && ReplChangeManager.shouldEnableCm(db, table.getTTable());
        }
      } else {
        loadFileType = replicationSpec.isReplace() ?
                LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING;
        //Replication scope the write id will be invalid
        boolean useStagingDirectory = !AcidUtils.isTransactionalTable(table.getParameters()) ||
                replicationSpec.isInReplicationScope();
        destPath = useStagingDirectory ? x.getCtx().getExternalTmpPath(tgtLocation)
                : new Path(tgtLocation, AcidUtils.deltaSubdir(writeId, writeId, stmtId));
      }

      Path moveTaskSrc = !AcidUtils.isTransactionalTable(table.getParameters()) ||
              replicationSpec.isInReplicationScope() ? destPath : tgtLocation;
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("adding import work for partition with source location: "
                + srcLocation + "; target: " + tgtLocation + "; copy dest " + destPath + "; mm "
                + writeId + " for " + partSpecToString(partSpec.getPartSpec()) + ": " +
                (AcidUtils.isFullAcidTable(table) ? "acid" :
                        (AcidUtils.isInsertOnlyTable(table) ? "mm" : "flat")
                )
        );
      }

      Task<?> copyTask = null;
      if (replicationSpec.isInReplicationScope()) {
        boolean copyAtLoad = x.getConf().getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
        copyTask = ReplCopyTask.getLoadCopyTask(replicationSpec, new Path(srcLocation), destPath,
                x.getConf(), isSkipTrash, needRecycle, copyAtLoad, dumpRoot, metricCollector);
      } else {
        copyTask = TaskFactory.get(new CopyWork(new Path(srcLocation), destPath, false,
                                                dumpRoot, metricCollector, isReplication));
      }

      Task<?> addPartTask = null;
      if (x.getEventType() != DumpType.EVENT_COMMIT_TXN) {
        // During replication, by the time we are applying commit transaction event, we expect
        // the partition/s to be already added or altered by previous events. So no need to
        // create add partition event again.
        addPartTask = TaskFactory.get(new DDLWork(x.getInputs(), x.getOutputs(), addPartitionDesc,
                                      isReplication, dumpRoot, metricCollector), x.getConf());
      }

      MoveWork moveWork = new MoveWork(x.getInputs(), x.getOutputs(),
              null, null, false, dumpRoot, metricCollector, isReplication);

      // Note: this sets LoadFileType incorrectly for ACID; is that relevant for import?
      //       See setLoadFileType and setIsAcidIow calls elsewhere for an example.
      if (replicationSpec.isInReplicationScope() && AcidUtils.isTransactionalTable(tblDesc.getTblProps())) {
        LoadMultiFilesDesc loadFilesWork = new LoadMultiFilesDesc(
                Collections.singletonList(destPath),
                Collections.singletonList(tgtLocation),
                true, null, null);
        moveWork.setMultiFilesDesc(loadFilesWork);
        moveWork.setNeedCleanTarget(replicationSpec.isReplace());
      } else {
        LoadTableDesc loadTableWork = new LoadTableDesc(moveTaskSrc, Utilities.getTableDesc(table),
                partSpec.getPartSpec(),
                loadFileType,
                writeId);
        loadTableWork.setStmtId(stmtId);
        loadTableWork.setInheritTableSpecs(false);
        moveWork.setLoadTableWork(loadTableWork);
      }

      if (loadFileType == LoadFileType.IGNORE) {
        // if file is coped directly to the target location, then no need of move task in case the operation getting
        // replayed is add partition. As add partition will add the event for create partition. Even the statics are
        // updated properly in create partition flow as the copy is done directly to the partition location. For insert
        // operations, add partition task is anyways a no-op as alter partition operation does just some statistics
        // update which is again done in load operations as part of move task.
        if (x.getEventType() == DumpType.EVENT_INSERT) {
          copyTask.addDependentTask(TaskFactory.get(moveWork, x.getConf()));
        } else {
          if (addPartTask != null) {
            copyTask.addDependentTask(addPartTask);
          }
        }
        return copyTask;
      }
      Task<?> loadPartTask = TaskFactory.get(moveWork, x.getConf());
      copyTask.addDependentTask(loadPartTask);
      if (addPartTask != null) {
        addPartTask.addDependentTask(loadPartTask);
        x.getTasks().add(copyTask);
        return addPartTask;
      }
      return copyTask;
    }
  }

  /**
   * In REPL LOAD flow, the data copy is done separately for external tables using data locations
   * dumped in file {@link ReplExternalTables#FILE_NAME}. So, we can skip copying it here.
   * In case of migrating from managed to external table, the data path won't be listed in this
   * file and so need to copy data while applying the event.
   */
  private static boolean shouldSkipDataCopyInReplScope(ImportTableDesc tblDesc, ReplicationSpec replicationSpec) {
    return ((replicationSpec != null)
            && replicationSpec.isInReplicationScope()
            && tblDesc.isExternal());
  }

  /**
   * Helper method to set location properly in partSpec
   */
  private static void fixLocationInPartSpec(ImportTableDesc tblDesc, Table table,
      Warehouse wh, ReplicationSpec replicationSpec, AlterTableAddPartitionDesc.PartitionDesc partSpec,
      EximUtil.SemanticAnalyzerWrapperContext x) throws MetaException, HiveException, IOException {
    if (externalTablePartition(tblDesc, replicationSpec)) {
      /*
        we use isExternal and not tableType() method since that always gives type as managed table.
        we don't do anything since for external table partitions the path is already set correctly
        in {@link org.apache.hadoop.hive.ql.parse.repl.load.message.TableHandler}
       */
      return;
    }
    Path tgtPath;
    if (tblDesc.getLocation() == null) {
      if (table.getDataLocation() != null) {
        tgtPath = new Path(table.getDataLocation().toString(),
            Warehouse.makePartPath(partSpec.getPartSpec()));
      } else {
        Database parentDb = x.getHive().getDatabase(tblDesc.getDatabaseName());
        tgtPath = new Path(
            wh.getDefaultTablePath( parentDb, tblDesc.getTableName(), tblDesc.isExternal()),
            Warehouse.makePartPath(partSpec.getPartSpec()));
      }
    } else {
      tgtPath = new Path(tblDesc.getLocation(),
          Warehouse.makePartPath(partSpec.getPartSpec()));
    }
    FileSystem tgtFs = FileSystem.get(tgtPath.toUri(), x.getConf());
    checkTargetLocationEmpty(tgtFs, tgtPath, replicationSpec, x.getLOG());
    partSpec.setLocation(tgtPath.toString());
  }

  private static boolean externalTablePartition(ImportTableDesc tblDesc,
      ReplicationSpec replicationSpec) {
    return (replicationSpec != null) && replicationSpec.isInReplicationScope()
        && tblDesc.isExternal();
  }

  public static void checkTargetLocationEmpty(FileSystem fs, Path targetPath, ReplicationSpec replicationSpec,
      Logger logger)
      throws IOException, SemanticException {
    if (replicationSpec.isInReplicationScope()){
      // replication scope allows replacement, and does not require empty directories
      return;
    }
    logger.debug("checking emptiness of " + targetPath.toString());
    if (fs.exists(targetPath)) {
      FileStatus[] status = fs.listStatus(targetPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      if (status.length > 0) {
        logger.debug("Files inc. " + status[0].getPath().toString()
            + " found in path : " + targetPath.toString());
        throw new SemanticException(ErrorMsg.TABLE_DATA_EXISTS.getMsg());
      }
    }
  }

  public static String partSpecToString(Map<String, String> partSpec) {
    StringBuilder sb = new StringBuilder();
    boolean firstTime = true;
    for (Map.Entry<String, String> entry : partSpec.entrySet()) {
      if (!firstTime) {
        sb.append(',');
      }
      firstTime = false;
      sb.append(entry.getKey());
      sb.append('=');
      sb.append(entry.getValue());
    }
    return sb.toString();
  }

  private static void checkTable(Table table, ImportTableDesc tableDesc,
      ReplicationSpec replicationSpec, HiveConf conf)
      throws SemanticException, URISyntaxException {
    // This method gets called only in the scope that a destination table already exists, so
    // we're validating if the table is an appropriate destination to import into

    if (replicationSpec.isInReplicationScope()){
      // If this import is being done for replication, then this will be a managed table, and replacements
      // are allowed irrespective of what the table currently looks like. So no more checks are necessary.
      return;
    } else {
      // verify if table has been the target of replication, and if so, check HiveConf if we're allowed
      // to override. If not, fail.
      if (table.getParameters().containsKey(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString())
          && conf.getBoolVar(HiveConf.ConfVars.HIVE_EXIM_RESTRICT_IMPORTS_INTO_REPLICATED_TABLES)){
            throw new SemanticException(ErrorMsg.IMPORT_INTO_STRICT_REPL_TABLE.getMsg(
                "Table "+table.getTableName()+" has repl.last.id parameter set." ));
      }
    }

    // Next, we verify that the destination table is not offline, or a non-native table
    EximUtil.validateTable(table);

    // If the import statement specified that we're importing to an external
    // table, we seem to be doing the following:
    //    a) We don't allow replacement in an unpartitioned pre-existing table
    //    b) We don't allow replacement in a partitioned pre-existing table where that table is external
    // TODO : Does this simply mean we don't allow replacement in external tables if they already exist?
    //    If so(i.e. the check is superfluous and wrong), this can be a simpler check. If not, then
    //    what we seem to be saying is that the only case we allow is to allow an IMPORT into an EXTERNAL
    //    table in the statement, if a destination partitioned table exists, so long as it is actually
    //    not external itself. Is that the case? Why?
    {
      if ((tableDesc.isExternal()) // IMPORT statement specified EXTERNAL
          && (!table.isPartitioned() || !table.getTableType().equals(TableType.EXTERNAL_TABLE))
          ){
        throw new SemanticException(ErrorMsg.INCOMPATIBLE_SCHEMA.getMsg(
            " External table cannot overwrite existing table. Drop existing table first."));
      }
    }

    // If a table import statement specified a location and the table(unpartitioned)
    // already exists, ensure that the locations are the same.
    // Partitioned tables not checked here, since the location provided would need
    // checking against the partition in question instead.
    {
      if ((tableDesc.getLocation() != null)
          && (!table.isPartitioned())
          && (!table.getDataLocation().equals(new Path(tableDesc.getLocation()))) ){
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA.getMsg(" Location does not match"));

      }
    }
    {
      // check column order and types
      List<FieldSchema> existingTableCols = table.getCols();
      List<FieldSchema> importedTableCols = tableDesc.getCols();
      if (!EximUtil.schemaCompare(importedTableCols, existingTableCols)) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Column Schema does not match"));
      }
    }
    {
      // check partitioning column order and types
      List<FieldSchema> existingTablePartCols = table.getPartCols();
      List<FieldSchema> importedTablePartCols = tableDesc.getPartCols();
      if (!EximUtil.schemaCompare(importedTablePartCols, existingTablePartCols)) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Partition Schema does not match"));
      }
    }
    {
      // check table params
      Map<String, String> existingTableParams = table.getParameters();
      Map<String, String> importedTableParams = tableDesc.getTblProps();
      String error = checkParams(existingTableParams, importedTableParams,
          new String[] { "howl.isd",
              "howl.osd" });
      if (error != null) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Table parameters do not match: " + error));
      }
    }
    {
      // check IF/OF/Serde
      String existingifc = table.getInputFormatClass().getName();
      String importedifc = tableDesc.getInputFormat();
      String existingofc = table.getOutputFormatClass().getName();
      String importedofc = tableDesc.getOutputFormat();
      /*
       * substitute OutputFormat name based on HiveFileFormatUtils.outputFormatSubstituteMap
       */
      try {
        Class<?> origin = Class.forName(importedofc, true, Utilities.getSessionSpecifiedClassLoader());
        Class<? extends OutputFormat> replaced = HiveFileFormatUtils
            .getOutputFormatSubstitute(origin);
        if (replaced == null) {
          throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
        }
        importedofc = replaced.getCanonicalName();
      } catch(Exception e) {
        throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
      }
      if ((!existingifc.equals(importedifc))
          || (!existingofc.equals(importedofc))) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Table inputformat/outputformats do not match"));
      }
      String existingSerde = table.getSerializationLib();
      String importedSerde = tableDesc.getSerName();
      if (!existingSerde.equals(importedSerde)) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Table Serde class does not match"));
      }
      String existingSerdeFormat = table
          .getSerdeParam(serdeConstants.SERIALIZATION_FORMAT);
      String importedSerdeFormat = tableDesc.getSerdeProps().get(
          serdeConstants.SERIALIZATION_FORMAT);

      /* TODO : Remove this weirdity. See notes in Table.getEmptyTable()
       * If Imported SerdeFormat is null, then set it to "1" just as
       * metadata.Table.getEmptyTable
       */
      importedSerdeFormat = importedSerdeFormat == null ? "1" : importedSerdeFormat;
      if (!TxnUtils.isTransactionalTable(table.getParameters()) &&
          !ObjectUtils.equals(existingSerdeFormat, importedSerdeFormat)) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Table Serde format does not match. Imported :"
                    + " "+importedSerdeFormat + " existing: " + existingSerdeFormat));
      }
    }
    {
      // check bucket/sort cols
      if (!ObjectUtils.equals(table.getBucketCols(), tableDesc.getBucketCols())) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Table bucketing spec does not match"));
      }
      List<Order> existingOrder = table.getSortCols();
      List<Order> importedOrder = tableDesc.getSortCols();
      // safely sorting
      final class OrderComparator implements Comparator<Order> {
        @Override
        public int compare(Order o1, Order o2) {
          if (o1.getOrder() < o2.getOrder()) {
            return -1;
          } else {
            if (o1.getOrder() == o2.getOrder()) {
              return 0;
            } else {
              return 1;
            }
          }
        }
      }
      if (existingOrder != null) {
        if (importedOrder != null) {
          Collections.sort(existingOrder, new OrderComparator());
          Collections.sort(importedOrder, new OrderComparator());
          if (!existingOrder.equals(importedOrder)) {
            throw new SemanticException(
                ErrorMsg.INCOMPATIBLE_SCHEMA
                    .getMsg(" Table sorting spec does not match"));
          }
        }
      } else {
        if (importedOrder != null) {
          throw new SemanticException(
              ErrorMsg.INCOMPATIBLE_SCHEMA
                  .getMsg(" Table sorting spec does not match"));
        }
      }
    }
  }

  private static String checkParams(Map<String, String> map1,
      Map<String, String> map2, String[] keys) {
    if (map1 != null) {
      if (map2 != null) {
        for (String key : keys) {
          String v1 = map1.get(key);
          String v2 = map2.get(key);
          if (!ObjectUtils.equals(v1, v2)) {
            return "Mismatch for " + key;
          }
        }
      } else {
        for (String key : keys) {
          if (map1.get(key) != null) {
            return "Mismatch for " + key;
          }
        }
      }
    } else {
      if (map2 != null) {
        for (String key : keys) {
          if (map2.get(key) != null) {
            return "Mismatch for " + key;
          }
        }
      }
    }
    return null;
  }

  /**
   * Create tasks for regular import, no repl complexity
   * @param tblDesc
   * @param partitionDescs
   * @param isPartSpecSet
   * @param replicationSpec
   * @param table
   * @param fromURI
   * @param fs
   * @param wh
   */
  private static void createRegularImportTasks(
      ImportTableDesc tblDesc, List<AlterTableAddPartitionDesc> partitionDescs, boolean isPartSpecSet,
      ReplicationSpec replicationSpec, Table table, URI fromURI, FileSystem fs, Warehouse wh,
      EximUtil.SemanticAnalyzerWrapperContext x, Long writeId, int stmtId)
      throws HiveException, IOException, MetaException {

    if (table != null) {
      if (table.isPartitioned()) {
        x.getLOG().debug("table partitioned");

        for (AlterTableAddPartitionDesc addPartitionDesc : partitionDescs) {
          Map<String, String> partSpec = addPartitionDesc.getPartitions().get(0).getPartSpec();
          org.apache.hadoop.hive.ql.metadata.Partition ptn = null;
          if ((ptn = x.getHive().getPartition(table, partSpec, false)) == null) {
            x.getTasks().add(addSinglePartition(
                tblDesc, table, wh, addPartitionDesc, replicationSpec, x, writeId, stmtId,
                    false, null, null));
          } else {
            throw new SemanticException(
                ErrorMsg.PARTITION_EXISTS.getMsg(partSpecToString(partSpec)));
          }
        }
      } else {
        x.getLOG().debug("table non-partitioned");
        // ensure if destination is not empty only for regular import
        Path tgtPath = new Path(table.getDataLocation().toString());
        FileSystem tgtFs = FileSystem.get(tgtPath.toUri(), x.getConf());
        checkTargetLocationEmpty(tgtFs, tgtPath, replicationSpec, x.getLOG());
        loadTable(fromURI, tblDesc, false, tgtPath, replicationSpec, x, writeId, stmtId);
      }
      // Set this to read because we can't overwrite any existing partitions
      x.getOutputs().add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
    } else {
      x.getLOG().debug("table " + tblDesc.getTableName() + " does not exist");

      Task<?> t = createTableTask(tblDesc, x);
      table = createNewTableMetadataObject(tblDesc, false);

      Database parentDb = x.getHive().getDatabase(tblDesc.getDatabaseName());

      // Since we are going to be creating a new table in a db, we should mark that db as a write entity
      // so that the auth framework can go to work there.
      x.getOutputs().add(new WriteEntity(parentDb, WriteEntity.WriteType.DDL_SHARED));

      if (isPartitioned(tblDesc)) {
        for (AlterTableAddPartitionDesc addPartitionDesc : partitionDescs) {
          t.addDependentTask(addSinglePartition(tblDesc, table, wh, addPartitionDesc,
            replicationSpec, x, writeId, stmtId, false, null, null));
        }
      } else {
        x.getLOG().debug("adding dependent CopyWork/MoveWork for table");
        if (tblDesc.isExternal() && (tblDesc.getLocation() == null)) {
          x.getLOG().debug("Importing in place, no emptiness check, no copying/loading");
          Path dataPath = new Path(fromURI.toString(), EximUtil.DATA_PATH_NAME);
          tblDesc.setLocation(dataPath.toString());
        } else {
          Path tablePath = null;
          if (tblDesc.getLocation() != null) {
            tablePath = new Path(tblDesc.getLocation());
          } else {
            tablePath = wh.getDefaultTablePath(parentDb, tblDesc.getTableName(), tblDesc.isExternal());
          }
          FileSystem tgtFs = FileSystem.get(tablePath.toUri(), x.getConf());
          checkTargetLocationEmpty(tgtFs, tablePath, replicationSpec,x.getLOG());
          t.addDependentTask(loadTable(fromURI, tblDesc, false, tablePath, replicationSpec, x,
              writeId, stmtId));
        }
      }
      x.getTasks().add(t);
    }
  }

  public static Table createNewTableMetadataObject(ImportTableDesc tblDesc, boolean isRepl)
      throws SemanticException {
    Table newTable = new Table(tblDesc.getDatabaseName(), tblDesc.getTableName());
    //so that we know the type of table we are creating: acid/MM to match what was exported
    newTable.setParameters(tblDesc.getTblProps());
    if(tblDesc.isExternal() && AcidUtils.isTransactionalTable(newTable)) {
      if (isRepl) {
        throw new SemanticException("External tables may not be transactional: " +
            Warehouse.getQualifiedName(tblDesc.getDatabaseName(), tblDesc.getTableName()));
      } else {
        throw new AssertionError("Internal error: transactional properties not set properly"
            + tblDesc.getTblProps());
      }
    }
    return newTable;
  }

  /**
   * Create tasks for repl import
   */
  private static void createReplImportTasks(
      ImportTableDesc tblDesc,
      List<AlterTableAddPartitionDesc> partitionDescs,
      ReplicationSpec replicationSpec, boolean waitOnPrecursor,
      Table table, URI fromURI, Warehouse wh,
      EximUtil.SemanticAnalyzerWrapperContext x, Long writeId, int stmtId,
      UpdatedMetaDataTracker updatedMetadata, String dumpRoot,
      ReplicationMetricCollector metricCollector)
      throws HiveException, IOException, MetaException {

    Task<?> dropTblTask = null;
    WriteEntity.WriteType lockType = WriteEntity.WriteType.DDL_NO_LOCK;
    boolean firstIncPending;

    // Normally, on import, trying to create a table or a partition in a db that does not yet exist
    // is a error condition. However, in the case of a REPL LOAD, it is possible that we are trying
    // to create tasks to create a table inside a db that as-of-now does not exist, but there is
    // a precursor Task waiting that will create it before this is encountered. Thus, we instantiate
    // defaults and do not error out in that case.
    Database parentDb = x.getHive().getDatabase(tblDesc.getDatabaseName());
    if (parentDb == null){
      if (!waitOnPrecursor){
        throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(tblDesc.getDatabaseName()));
      }
      // For warehouse level replication, if the database itself is getting created in this load, then no need to
      // check for duplicate copy. Check HIVE-21197 for more detail.
      firstIncPending = false;
    } else {
      // For database replication, get the flag from database parameter. Check HIVE-21197 for more detail.
      firstIncPending = ReplUtils.isFirstIncPending(parentDb.getParameters());
    }

    if (table != null) {
      if (!replicationSpec.allowReplacementInto(parentDb.getParameters())) {
        // If the target table exists and is newer or same as current update based on repl.last.id, then just noop it.
        x.getLOG().info("Table {}.{} is not replaced as it is newer than the update",
                tblDesc.getDatabaseName(), tblDesc.getTableName());
        return;
      }

      // If the table exists and we found a valid create table event, then need to drop the table first
      // and then create it. This case is possible if the event sequence is drop_table(t1) -> create_table(t1).
      // We need to drop here to handle the case where the previous incremental load created the table but
      // didn't set the last repl ID due to some failure.
      if (x.getEventType() == DumpType.EVENT_CREATE_TABLE) {
        dropTblTask = dropTableTask(table, x, replicationSpec, dumpRoot, metricCollector);
        table = null;
      } else if (!firstIncPending) {
        //If in db pending flag is not set then check in table parameter for table level load.
        // Check HIVE-21197 for more detail.
        firstIncPending = ReplUtils.isFirstIncPending(table.getParameters());
      }
    } else {
      // If table doesn't exist, allow creating a new one only if the database state is older than the update.
      if ((parentDb != null) && (!replicationSpec.allowReplacementInto(parentDb.getParameters()))) {
        // If the target table exists and is newer or same as current update based on repl.last.id, then just noop it.
        x.getLOG().info("Table {}.{} is not created as the database is newer than the update",
                tblDesc.getDatabaseName(), tblDesc.getTableName());
        return;
      }
    }

    // For first incremental load just after bootstrap, we need to check for duplicate copy.
    // Check HIVE-21197 for more detail.
    replicationSpec.setNeedDupCopyCheck(firstIncPending);

    if (updatedMetadata != null) {
      updatedMetadata.set(replicationSpec.getReplicationState(),
                          tblDesc.getDatabaseName(),
                          tblDesc.getTableName(),
                          null);
    }

    if (tblDesc.getLocation() == null) {
      if (parentDb != null && !tblDesc.isExternal() && StringUtils.isNotBlank(parentDb.getManagedLocationUri())) {
        tblDesc.setLocation(new Path(parentDb.getManagedLocationUri(), tblDesc.getTableName()).toString());
        LOG.info("Setting the location for table {} as {}", tblDesc.getTableName(), tblDesc.getLocation());
      } else if (!waitOnPrecursor) {
        tblDesc.setLocation(wh.getDefaultTablePath(parentDb, tblDesc.getTableName(), tblDesc.isExternal()).toString());
      } else {
        tblDesc.setLocation(
                wh.getDnsPath(wh.getDefaultTablePath(tblDesc.getDatabaseName(), tblDesc.getTableName(),
                        tblDesc.isExternal())).toString());
      }
    }

    /* Note: In the following section, Metadata-only import handling logic is
       interleaved with regular repl-import logic. The rule of thumb being
       followed here is that MD-only imports are essentially ALTERs. They do
       not load data, and should not be "creating" any metadata - they should
       be replacing instead. The only place it makes sense for a MD-only import
       to create is in the case of a table that's been dropped and recreated,
       or in the case of an unpartitioned table. In all other cases, it should
       behave like a noop or a pure MD alter.
    */
    if (table == null) {
      if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
        lockType = WriteEntity.WriteType.DDL_SHARED;
      }

      table = createNewTableMetadataObject(tblDesc, true);

      List<Task<?>> dependentTasks = null;
      if (isPartitioned(tblDesc)) {
        dependentTasks = new ArrayList<>(partitionDescs.size());
        for (AlterTableAddPartitionDesc addPartitionDesc : partitionDescs) {
          addPartitionDesc.setReplicationSpec(replicationSpec);
          if (!replicationSpec.isMetadataOnly()) {
            dependentTasks.add(addSinglePartition(tblDesc, table, wh, addPartitionDesc,
                                                replicationSpec, x, writeId, stmtId,
                                                true, dumpRoot, metricCollector));
          } else {
            dependentTasks.add(alterSinglePartition(tblDesc, table, wh, addPartitionDesc,
                                                  replicationSpec, null, x, true,
                                                  dumpRoot, metricCollector));
          }
          if (updatedMetadata != null) {
            updatedMetadata.addPartition(table.getDbName(), table.getTableName(),
                    addPartitionDesc.getPartitions().get(0).getPartSpec());
          }
        }
      } else if (!replicationSpec.isMetadataOnly()
              && !shouldSkipDataCopyInReplScope(tblDesc, replicationSpec)) {
        x.getLOG().debug("adding dependent CopyWork/MoveWork for table");
        dependentTasks = Collections.singletonList(loadTable(fromURI, tblDesc, replicationSpec.isReplace(),
            new Path(tblDesc.getLocation()), replicationSpec, x, writeId, stmtId, dumpRoot, metricCollector));
      }

      // During replication, by the time we replay a commit transaction event, the table should
      // have been already created when replaying previous events. So no need to create table
      // again.
      if (x.getEventType() != DumpType.EVENT_COMMIT_TXN) {
        //Don't set location for managed tables while creating the table.
        if (x.getEventType() == DumpType.EVENT_CREATE_TABLE && !tblDesc.isExternal()) {
          tblDesc.setLocation(null);
        }
        Task t = createTableTask(tblDesc, x, dumpRoot, metricCollector);
        if (dependentTasks != null) {
          dependentTasks.forEach(task -> t.addDependentTask(task));
        }
        if (dropTblTask != null) {
          // Drop first and then create
          dropTblTask.addDependentTask(t);
          x.getTasks().add(dropTblTask);
        } else {
          // Simply create
          x.getTasks().add(t);
        }
      } else {
        // We should not require to create a drop table task when replaying a commit transaction
        // event. That should have been done when replaying create table event itself.
        assert dropTblTask == null;

        // Add all the tasks created above directly
        if (dependentTasks != null) {
          x.getTasks().addAll(dependentTasks);
        }
      }
    } else {
      // If table of current event has partition flag different from existing table, it means, some
      // of the previous events in same batch have drop and create table events with same same but
      // different partition flag. In this case, should go with current event's table type and so
      // create the dummy table object for adding repl tasks.
      boolean isOldTableValid = true;
      if (table.isPartitioned() != isPartitioned(tblDesc)) {
        table = createNewTableMetadataObject(tblDesc, true);
        isOldTableValid = false;
      }
      // Table existed, and is okay to replicate into, not dropping and re-creating.
      if (isPartitioned(tblDesc)) {
        x.getLOG().debug("table partitioned");
        for (AlterTableAddPartitionDesc addPartitionDesc : partitionDescs) {
          addPartitionDesc.setReplicationSpec(replicationSpec);
          Map<String, String> partSpec = addPartitionDesc.getPartitions().get(0).getPartSpec();
          org.apache.hadoop.hive.ql.metadata.Partition ptn = null;
          if (isOldTableValid) {
            // If existing table is valid but the partition spec is different, then ignore partition
            // validation and create new partition.
            try {
              ptn = x.getHive().getPartition(table, partSpec, false);
            } catch (HiveException ex) {
              ptn = null;
              table = createNewTableMetadataObject(tblDesc, true);
              isOldTableValid = false;
            }
          }

          if (ptn == null) {
            if (!replicationSpec.isMetadataOnly()){
              x.getTasks().add(addSinglePartition(
                  tblDesc, table, wh, addPartitionDesc, replicationSpec, x, writeId, stmtId,
                      true, dumpRoot, metricCollector));
              if (updatedMetadata != null) {
                updatedMetadata.addPartition(table.getDbName(), table.getTableName(), partSpec);
              }
            } else {
              x.getTasks().add(alterSinglePartition(
                  tblDesc, table, wh, addPartitionDesc, replicationSpec, null, x,
                      true, dumpRoot, metricCollector));
              if (updatedMetadata != null) {
                updatedMetadata.addPartition(table.getDbName(), table.getTableName(), partSpec);
              }
            }
          } else {
            // If replicating, then the partition already existing means we need to replace, maybe, if
            // the destination ptn's repl.last.id is older than the replacement's.
            if (replicationSpec.allowReplacementInto(parentDb.getParameters())){
              if (!replicationSpec.isMetadataOnly()){
                x.getTasks().add(addSinglePartition(tblDesc, table, wh, addPartitionDesc, replicationSpec, x,
                                                    writeId, stmtId, true, dumpRoot, metricCollector));
              } else {
                x.getTasks().add(alterSinglePartition(
                    tblDesc, table, wh, addPartitionDesc, replicationSpec, ptn, x, true, dumpRoot, metricCollector));
              }
              if (updatedMetadata != null) {
                updatedMetadata.addPartition(table.getDbName(), table.getTableName(), partSpec);
              }
              if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
                lockType = WriteEntity.WriteType.DDL_SHARED;
              }
            }
          }
        }
        if (replicationSpec.isMetadataOnly() && partitionDescs.isEmpty()){
          // MD-ONLY table alter
          x.getTasks().add(alterTableTask(tblDesc, x,replicationSpec, true, dumpRoot, metricCollector));
          if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
            lockType = WriteEntity.WriteType.DDL_SHARED;
          }
        }
      } else {
        x.getLOG().debug("table non-partitioned");
        if (!replicationSpec.isMetadataOnly()) {
          // repl-imports are replace-into unless the event is insert-into
          loadTable(fromURI, tblDesc, replicationSpec.isReplace(), new Path(tblDesc.getLocation()),
            replicationSpec, x, writeId, stmtId, dumpRoot, metricCollector);
        } else {
          x.getTasks().add(alterTableTask(tblDesc, x, replicationSpec, true, dumpRoot, metricCollector));
        }
        if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
          lockType = WriteEntity.WriteType.DDL_SHARED;
        }
      }
    }
    x.getOutputs().add(new WriteEntity(table,lockType));
  }

  public static boolean isPartitioned(ImportTableDesc tblDesc) {
    return !(tblDesc.getPartCols() == null || tblDesc.getPartCols().isEmpty());
  }

  /**
   * Utility method that returns a table if one corresponding to the destination
   * tblDesc is found. Returns null if no such table is found.
   */
  public static Table tableIfExists(ImportTableDesc tblDesc, Hive db) throws HiveException {
    try {
      return db.getTable(tblDesc.getDatabaseName(),tblDesc.getTableName());
    } catch (InvalidTableException e) {
      return null;
    }
  }

}
