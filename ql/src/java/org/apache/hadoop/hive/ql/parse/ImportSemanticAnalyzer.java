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
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ImportCommitWork;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.OutputFormat;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Serializable;
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

      for (int i = 1; i < ast.getChildCount(); ++i){
        ASTNode child = (ASTNode) ast.getChild(i);
        switch (child.getToken().getType()){
          case HiveParser.KW_EXTERNAL:
            isExternalSet = true;
            break;
          case HiveParser.TOK_TABLELOCATION:
            isLocationSet = true;
            parsedLocation = EximUtil.relativeToAbsolutePath(conf, unescapeSQLString(child.getChild(0).getText()));
            break;
          case HiveParser.TOK_TAB:
            ASTNode tableNameNode = (ASTNode) child.getChild(0);
            Map.Entry<String,String> dbTablePair = getDbTableNamePair(tableNameNode);
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
      tableExists = prepareImport(true,
          isLocationSet, isExternalSet, isPartSpecSet, waitOnPrecursor,
          parsedLocation, parsedTableName, parsedDbName, parsedPartSpec, fromTree.getText(),
          new EximUtil.SemanticAnalyzerWrapperContext(conf, db, inputs, outputs, rootTasks, LOG, ctx),
          null);

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
   *
   * hence for import to work correctly we have to pass in the sessionState default Db via the
   * parsedDbName parameter
   */
  public static boolean prepareImport(boolean isImportCmd,
      boolean isLocationSet, boolean isExternalSet, boolean isPartSpecSet, boolean waitOnPrecursor,
      String parsedLocation, String parsedTableName, String overrideDBName,
      LinkedHashMap<String, String> parsedPartSpec,
      String fromLocn, EximUtil.SemanticAnalyzerWrapperContext x,
      UpdatedMetaDataTracker updatedMetadata
  ) throws IOException, MetaException, HiveException, URISyntaxException {

    // initialize load path
    URI fromURI = EximUtil.getValidatedURI(x.getConf(), stripQuotes(fromLocn));
    Path fromPath = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());

    FileSystem fs = FileSystem.get(fromURI, x.getConf());
    x.getInputs().add(toReadEntity(fromPath, x.getConf()));

    MetaData rv;
    try {
      rv =  EximUtil.readMetaData(fs, new Path(fromPath, EximUtil.METADATA_NAME));
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }

    if (rv.getTable() == null) {
      // nothing to do here, silently return.
      return false;
    }

    ReplicationSpec replicationSpec = rv.getReplicationSpec();
    if (replicationSpec.isNoop()){
      // nothing to do here, silently return.
      x.getLOG().debug("Current update with ID:{} is noop",
                                  replicationSpec.getCurrentReplicationState());
      return false;
    }

    if (isImportCmd) {
      replicationSpec.setReplSpecType(ReplicationSpec.Type.IMPORT);
    }

    String dbname = rv.getTable().getDbName();
    if ((overrideDBName !=null) && (!overrideDBName.isEmpty())){
      // If the parsed statement contained a db.tablename specification, prefer that.
      dbname = overrideDBName;
    }

    // Create table associated with the import
    // Executed if relevant, and used to contain all the other details about the table if not.
    ImportTableDesc tblDesc;
    try {
      tblDesc = getBaseCreateTableDescFromTable(dbname, rv.getTable());
    } catch (Exception e) {
      throw new HiveException(e);
    }
    boolean isSourceMm = AcidUtils.isInsertOnlyTable(tblDesc.getTblProps());

    if ((replicationSpec != null) && replicationSpec.isInReplicationScope()){
      tblDesc.setReplicationSpec(replicationSpec);
      StatsSetupConst.setBasicStatsState(tblDesc.getTblProps(), StatsSetupConst.FALSE);
    }

    if (isExternalSet){
      if (isSourceMm) {
        throw new SemanticException("Cannot import an MM table as external");
      }
      tblDesc.setExternal(isExternalSet);
      // This condition-check could have been avoided, but to honour the old
      // default of not calling if it wasn't set, we retain that behaviour.
      // TODO:cleanup after verification that the outer if isn't really needed here
    }

    if (isLocationSet){
      tblDesc.setLocation(parsedLocation);
      x.getInputs().add(toReadEntity(new Path(parsedLocation), x.getConf()));
    }

    if ((parsedTableName!= null) && (!parsedTableName.isEmpty())){
      tblDesc.setTableName(parsedTableName);
    }

    List<AddPartitionDesc> partitionDescs = new ArrayList<AddPartitionDesc>();
    Iterable<Partition> partitions = rv.getPartitions();
    for (Partition partition : partitions) {
      // TODO: this should ideally not create AddPartitionDesc per partition
      AddPartitionDesc partsDesc = getBaseAddPartitionDescFromPartition(fromPath, dbname, tblDesc, partition);
      if ((replicationSpec != null) && replicationSpec.isInReplicationScope()){
        StatsSetupConst.setBasicStatsState(partsDesc.getPartition(0).getPartParams(), StatsSetupConst.FALSE);
      }
      partitionDescs.add(partsDesc);
    }

    if (isPartSpecSet){
      // The import specification asked for only a particular partition to be loaded
      // We load only that, and ignore all the others.
      boolean found = false;
      for (Iterator<AddPartitionDesc> partnIter = partitionDescs
          .listIterator(); partnIter.hasNext();) {
        AddPartitionDesc addPartitionDesc = partnIter.next();
        if (!found && addPartitionDesc.getPartition(0).getPartSpec().equals(parsedPartSpec)) {
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

    if (tblDesc.getTableName() == null) {
      // Either we got the tablename from the IMPORT statement (first priority)
      // or from the export dump.
      throw new SemanticException(ErrorMsg.NEED_TABLE_SPECIFICATION.getMsg());
    } else {
      x.getConf().set("import.destination.table", tblDesc.getTableName());
      for (AddPartitionDesc addPartitionDesc : partitionDescs) {
        addPartitionDesc.setTableName(tblDesc.getTableName());
      }
    }

    Warehouse wh = new Warehouse(x.getConf());
    Table table = tableIfExists(tblDesc, x.getHive());
    boolean tableExists = false;

    if (table != null){
      checkTable(table, tblDesc,replicationSpec, x.getConf());
      x.getLOG().debug("table " + tblDesc.getTableName() + " exists: metadata checked");
      tableExists = true;
    }

    Long txnId = SessionState.get().getTxnMgr().getCurrentTxnId();
    int stmtId = 0;
    // TODO [MM gap?]: bad merge; tblDesc is no longer CreateTableDesc, but ImportTableDesc.
    //                 We need to verify the tests to see if this works correctly.
    /*
    if (isAcid(txnId)) {
      tblDesc.setInitialMmWriteId(txnId);
    }
    */
    if (!replicationSpec.isInReplicationScope()) {
      createRegularImportTasks(
          tblDesc, partitionDescs,
          isPartSpecSet, replicationSpec, table,
          fromURI, fs, wh, x, txnId, stmtId, isSourceMm);
    } else {
      createReplImportTasks(
          tblDesc, partitionDescs,
          replicationSpec, waitOnPrecursor, table,
          fromURI, fs, wh, x, txnId, stmtId, isSourceMm, updatedMetadata);
    }
    return tableExists;
  }

  private static AddPartitionDesc getBaseAddPartitionDescFromPartition(
      Path fromPath, String dbname,
      ImportTableDesc tblDesc, Partition partition) throws MetaException, SemanticException {
    AddPartitionDesc partsDesc = new AddPartitionDesc(dbname, tblDesc.getTableName(),
        EximUtil.makePartSpec(tblDesc.getPartCols(), partition.getValues()),
        partition.getSd().getLocation(), partition.getParameters());
    AddPartitionDesc.OnePartitionDesc partDesc = partsDesc.getPartition(0);
    partDesc.setInputFormat(partition.getSd().getInputFormat());
    partDesc.setOutputFormat(partition.getSd().getOutputFormat());
    partDesc.setNumBuckets(partition.getSd().getNumBuckets());
    partDesc.setCols(partition.getSd().getCols());
    partDesc.setSerializationLib(partition.getSd().getSerdeInfo().getSerializationLib());
    partDesc.setSerdeParams(partition.getSd().getSerdeInfo().getParameters());
    partDesc.setBucketCols(partition.getSd().getBucketCols());
    partDesc.setSortCols(partition.getSd().getSortCols());
    partDesc.setLocation(new Path(fromPath,
        Warehouse.makePartName(tblDesc.getPartCols(), partition.getValues())).toString());
    return partsDesc;
  }

  private static ImportTableDesc getBaseCreateTableDescFromTable(String dbName,
      org.apache.hadoop.hive.metastore.api.Table tblObj) throws Exception {
    Table table = new Table(tblObj);
    ImportTableDesc tblDesc = new ImportTableDesc(dbName, table);
    return tblDesc;
  }


  private static Task<?> loadTable(URI fromURI, Table table, boolean replace, Path tgtPath,
      ReplicationSpec replicationSpec, EximUtil.SemanticAnalyzerWrapperContext x,
      Long txnId, int stmtId, boolean isSourceMm) {
    Path dataPath = new Path(fromURI.toString(), EximUtil.DATA_PATH_NAME);
    Path destPath = null, loadPath = null;
    LoadFileType lft;
    if (AcidUtils.isInsertOnlyTable(table)) {
      String mmSubdir = replace ? AcidUtils.baseDir(txnId)
          : AcidUtils.deltaSubdir(txnId, txnId, stmtId);
      destPath = new Path(tgtPath, mmSubdir);
      loadPath = tgtPath;
      lft = LoadFileType.KEEP_EXISTING;
    } else {
      destPath = loadPath = x.getCtx().getExternalTmpPath(tgtPath);
      lft = replace ? LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING;
    }


    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("adding import work for table with source location: " +
        dataPath + "; table: " + tgtPath + "; copy destination " + destPath + "; mm " + txnId +
        " (src " + isSourceMm + ") for " + (table == null ? "a new table" : table.getTableName()));
    }

    Task<?> copyTask = null;
    if (replicationSpec.isInReplicationScope()) {
      if (isSourceMm || isAcid(txnId)) {
        // Note: this is replication gap, not MM gap... Repl V2 is not ready yet.
        throw new RuntimeException("Replicating MM and ACID tables is not supported");
      }
      copyTask = ReplCopyTask.getLoadCopyTask(replicationSpec, dataPath, destPath, x.getConf());
    } else {
      CopyWork cw = new CopyWork(dataPath, destPath, false);
      cw.setSkipSourceMmDirs(isSourceMm);
      copyTask = TaskFactory.get(cw, x.getConf());
    }

    LoadTableDesc loadTableWork = new LoadTableDesc(
        loadPath, Utilities.getTableDesc(table), new TreeMap<>(), lft, txnId);
    loadTableWork.setStmtId(stmtId);
    MoveWork mv = new MoveWork(x.getInputs(), x.getOutputs(), loadTableWork, null, false);
    Task<?> loadTableTask = TaskFactory.get(mv, x.getConf());
    copyTask.addDependentTask(loadTableTask);
    x.getTasks().add(copyTask);
    return loadTableTask;
  }

  /**
   * todo: this is odd: transactions are opened for all statements.  what is this supposed to check?
   */
  @Deprecated
  private static boolean isAcid(Long txnId) {
    return (txnId != null) && (txnId != 0);
  }

  private static Task<?> createTableTask(ImportTableDesc tableDesc, EximUtil.SemanticAnalyzerWrapperContext x){
    return tableDesc.getCreateTableTask(x.getInputs(), x.getOutputs(), x.getConf());
  }

  private static Task<? extends Serializable> alterTableTask(ImportTableDesc tableDesc,
      EximUtil.SemanticAnalyzerWrapperContext x, ReplicationSpec replicationSpec) {
    tableDesc.setReplaceMode(true);
    if ((replicationSpec != null) && (replicationSpec.isInReplicationScope())){
      tableDesc.setReplicationSpec(replicationSpec);
    }
    return tableDesc.getCreateTableTask(x.getInputs(), x.getOutputs(), x.getConf());
  }

  private static Task<? extends Serializable> alterSinglePartition(
      URI fromURI, FileSystem fs, ImportTableDesc tblDesc,
      Table table, Warehouse wh, AddPartitionDesc addPartitionDesc,
      ReplicationSpec replicationSpec, org.apache.hadoop.hive.ql.metadata.Partition ptn,
      EximUtil.SemanticAnalyzerWrapperContext x) throws MetaException, IOException, HiveException {
    addPartitionDesc.setReplaceMode(true);
    if ((replicationSpec != null) && (replicationSpec.isInReplicationScope())){
      addPartitionDesc.setReplicationSpec(replicationSpec);
    }
    AddPartitionDesc.OnePartitionDesc partSpec = addPartitionDesc.getPartition(0);
    if (ptn == null) {
      fixLocationInPartSpec(fs, tblDesc, table, wh, replicationSpec, partSpec, x);
    } else {
      partSpec.setLocation(ptn.getLocation()); // use existing location
    }
    return TaskFactory.get(new DDLWork(
        x.getInputs(),
        x.getOutputs(),
        addPartitionDesc
    ), x.getConf());
  }

 private static Task<?> addSinglePartition(URI fromURI, FileSystem fs, ImportTableDesc tblDesc,
      Table table, Warehouse wh, AddPartitionDesc addPartitionDesc, ReplicationSpec replicationSpec,
      EximUtil.SemanticAnalyzerWrapperContext x, Long txnId, int stmtId, boolean isSourceMm,
      Task<?> commitTask)
      throws MetaException, IOException, HiveException {
    AddPartitionDesc.OnePartitionDesc partSpec = addPartitionDesc.getPartition(0);
    if (tblDesc.isExternal() && tblDesc.getLocation() == null) {
      x.getLOG().debug("Importing in-place: adding AddPart for partition "
          + partSpecToString(partSpec.getPartSpec()));
      // addPartitionDesc already has the right partition location
      @SuppressWarnings("unchecked")
      Task<?> addPartTask = TaskFactory.get(new DDLWork(x.getInputs(),
          x.getOutputs(), addPartitionDesc), x.getConf());
      return addPartTask;
    } else {
      String srcLocation = partSpec.getLocation();
      fixLocationInPartSpec(fs, tblDesc, table, wh, replicationSpec, partSpec, x);
      x.getLOG().debug("adding dependent CopyWork/AddPart/MoveWork for partition "
          + partSpecToString(partSpec.getPartSpec())
          + " with source location: " + srcLocation);
      Path tgtLocation = new Path(partSpec.getLocation());
      Path destPath = !AcidUtils.isInsertOnlyTable(table.getParameters()) ? x.getCtx().getExternalTmpPath(tgtLocation)
          : new Path(tgtLocation, AcidUtils.deltaSubdir(txnId, txnId, stmtId));
      Path moveTaskSrc =  !AcidUtils.isInsertOnlyTable(table.getParameters()) ? destPath : tgtLocation;
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("adding import work for partition with source location: "
          + srcLocation + "; target: " + tgtLocation + "; copy dest " + destPath + "; mm "
          + txnId + " (src " + isSourceMm + ") for " + partSpecToString(partSpec.getPartSpec()));
      }


      Task<?> copyTask = null;
      if (replicationSpec.isInReplicationScope()) {
        if (isSourceMm || isAcid(txnId)) {
          // Note: this is replication gap, not MM gap... Repl V2 is not ready yet.
          throw new RuntimeException("Replicating MM and ACID tables is not supported");
        }
        copyTask = ReplCopyTask.getLoadCopyTask(
            replicationSpec, new Path(srcLocation), destPath, x.getConf());
      } else {
        CopyWork cw = new CopyWork(new Path(srcLocation), destPath, false);
        cw.setSkipSourceMmDirs(isSourceMm);
        copyTask = TaskFactory.get(cw, x.getConf());
      }

      Task<?> addPartTask = TaskFactory.get(new DDLWork(x.getInputs(),
          x.getOutputs(), addPartitionDesc), x.getConf());
      LoadTableDesc loadTableWork = new LoadTableDesc(moveTaskSrc, Utilities.getTableDesc(table),
          partSpec.getPartSpec(),
          replicationSpec.isReplace() ? LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING,
          txnId);
      loadTableWork.setStmtId(stmtId);
      loadTableWork.setInheritTableSpecs(false);
      Task<?> loadPartTask = TaskFactory.get(new MoveWork(
          x.getInputs(), x.getOutputs(), loadTableWork, null, false), x.getConf());
      copyTask.addDependentTask(loadPartTask);
      addPartTask.addDependentTask(loadPartTask);
      x.getTasks().add(copyTask);
      if (commitTask != null) {
        loadPartTask.addDependentTask(commitTask);
      }
      return addPartTask;
    }
  }

  /**
   * Helper method to set location properly in partSpec
   */
  private static void fixLocationInPartSpec(
      FileSystem fs, ImportTableDesc tblDesc, Table table,
      Warehouse wh, ReplicationSpec replicationSpec,
      AddPartitionDesc.OnePartitionDesc partSpec,
      EximUtil.SemanticAnalyzerWrapperContext x) throws MetaException, HiveException, IOException {
    Path tgtPath = null;
    if (tblDesc.getLocation() == null) {
      if (table.getDataLocation() != null) {
        tgtPath = new Path(table.getDataLocation().toString(),
            Warehouse.makePartPath(partSpec.getPartSpec()));
      } else {
        Database parentDb = x.getHive().getDatabase(tblDesc.getDatabaseName());
        tgtPath = new Path(
            wh.getDefaultTablePath( parentDb, tblDesc.getTableName()),
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

  public static void checkTable(Table table, ImportTableDesc tableDesc,
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
      if (table.getParameters().containsKey(ReplicationSpec.KEY.CURR_STATE_ID.toString())
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
      if ( (tableDesc.isExternal()) // IMPORT statement speicified EXTERNAL
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
      /*
       * If Imported SerdeFormat is null, then set it to "1" just as
       * metadata.Table.getEmptyTable
       */
      importedSerdeFormat = importedSerdeFormat == null ? "1" : importedSerdeFormat;
      if (!ObjectUtils.equals(existingSerdeFormat, importedSerdeFormat)) {
        throw new SemanticException(
            ErrorMsg.INCOMPATIBLE_SCHEMA
                .getMsg(" Table Serde format does not match"));
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
      ImportTableDesc tblDesc, List<AddPartitionDesc> partitionDescs, boolean isPartSpecSet,
      ReplicationSpec replicationSpec, Table table, URI fromURI, FileSystem fs, Warehouse wh,
      EximUtil.SemanticAnalyzerWrapperContext x, Long txnId, int stmtId, boolean isSourceMm)
      throws HiveException, URISyntaxException, IOException, MetaException {

    if (table != null) {
      if (table.isPartitioned()) {
        x.getLOG().debug("table partitioned");
        Task<?> ict = createImportCommitTask(
            table.getDbName(), table.getTableName(), txnId, stmtId, x.getConf(),
            AcidUtils.isInsertOnlyTable(table.getParameters()));

        for (AddPartitionDesc addPartitionDesc : partitionDescs) {
          Map<String, String> partSpec = addPartitionDesc.getPartition(0).getPartSpec();
          org.apache.hadoop.hive.ql.metadata.Partition ptn = null;
          if ((ptn = x.getHive().getPartition(table, partSpec, false)) == null) {
            x.getTasks().add(addSinglePartition(
                fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec, x, txnId, stmtId, isSourceMm, ict));
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
        loadTable(fromURI, table, false, tgtPath, replicationSpec, x, txnId, stmtId, isSourceMm);
      }
      // Set this to read because we can't overwrite any existing partitions
      x.getOutputs().add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
    } else {
      x.getLOG().debug("table " + tblDesc.getTableName() + " does not exist");

      Task<?> t = createTableTask(tblDesc, x);
      table = new Table(tblDesc.getDatabaseName(), tblDesc.getTableName());
      Database parentDb = x.getHive().getDatabase(tblDesc.getDatabaseName());

      // Since we are going to be creating a new table in a db, we should mark that db as a write entity
      // so that the auth framework can go to work there.
      x.getOutputs().add(new WriteEntity(parentDb, WriteEntity.WriteType.DDL_SHARED));

      if (isPartitioned(tblDesc)) {
        Task<?> ict = createImportCommitTask(
            tblDesc.getDatabaseName(), tblDesc.getTableName(), txnId, stmtId, x.getConf(),
            AcidUtils.isInsertOnlyTable(tblDesc.getTblProps()));
        for (AddPartitionDesc addPartitionDesc : partitionDescs) {
          t.addDependentTask(addSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc,
            replicationSpec, x, txnId, stmtId, isSourceMm, ict));
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
            tablePath = wh.getDefaultTablePath(parentDb, tblDesc.getTableName());
          }
          FileSystem tgtFs = FileSystem.get(tablePath.toUri(), x.getConf());
          checkTargetLocationEmpty(tgtFs, tablePath, replicationSpec,x.getLOG());
          if (isSourceMm) { // since target table doesn't exist, it should inherit soruce table's properties
            Map<String, String> tblproperties = table.getParameters();
            tblproperties.put("transactional", "true");
            tblproperties.put("transactional_properties", "insert_only");
            table.setParameters(tblproperties);
          }
          t.addDependentTask(loadTable(fromURI, table, false, tablePath, replicationSpec, x, txnId, stmtId, isSourceMm));
        }
      }
      x.getTasks().add(t);
    }
  }

  private static Task<?> createImportCommitTask(
      String dbName, String tblName, Long txnId, int stmtId, HiveConf conf, boolean isMmTable) {
    // TODO: noop, remove?
    @SuppressWarnings("unchecked")
    Task<ImportCommitWork> ict = (!isMmTable) ? null : TaskFactory.get(
        new ImportCommitWork(dbName, tblName, txnId, stmtId), conf);
    return ict;
  }

  /**
   * Create tasks for repl import
   */
  private static void createReplImportTasks(
      ImportTableDesc tblDesc,
      List<AddPartitionDesc> partitionDescs,
      ReplicationSpec replicationSpec, boolean waitOnPrecursor,
      Table table, URI fromURI, FileSystem fs, Warehouse wh,
      EximUtil.SemanticAnalyzerWrapperContext x, Long txnId, int stmtId, boolean isSourceMm,
      UpdatedMetaDataTracker updatedMetadata)
      throws HiveException, URISyntaxException, IOException, MetaException {

    Task<?> dr = null;
    WriteEntity.WriteType lockType = WriteEntity.WriteType.DDL_NO_LOCK;

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
    }

    if (table != null) {
      if (!replicationSpec.allowReplacementInto(table.getParameters())) {
        // If the target table exists and is newer or same as current update based on repl.last.id, then just noop it.
        x.getLOG().info("Table {}.{} is not replaced as it is newer than the update",
                tblDesc.getDatabaseName(), tblDesc.getTableName());
        return;
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

    if (updatedMetadata != null) {
      updatedMetadata.set(replicationSpec.getReplicationState(),
                          tblDesc.getDatabaseName(),
                          tblDesc.getTableName(),
                          null);
    }

    if (tblDesc.getLocation() == null) {
      if (!waitOnPrecursor){
        tblDesc.setLocation(wh.getDefaultTablePath(parentDb, tblDesc.getTableName()).toString());
      } else {
        tblDesc.setLocation(
            wh.getDnsPath(new Path(
                wh.getDefaultDatabasePath(tblDesc.getDatabaseName()),
                org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.encodeTableName(tblDesc.getTableName().toLowerCase())
            )
        ).toString());

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

      Task t = createTableTask(tblDesc, x);
      table = new Table(tblDesc.getDatabaseName(), tblDesc.getTableName());

      if (!replicationSpec.isMetadataOnly()) {
        if (isPartitioned(tblDesc)) {
          Task<?> ict = createImportCommitTask(
              tblDesc.getDatabaseName(), tblDesc.getTableName(), txnId, stmtId, x.getConf(),
              AcidUtils.isInsertOnlyTable(tblDesc.getTblProps()));
          for (AddPartitionDesc addPartitionDesc : partitionDescs) {
            addPartitionDesc.setReplicationSpec(replicationSpec);
            t.addDependentTask(
                addSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec, x, txnId, stmtId, isSourceMm, ict));
            if (updatedMetadata != null) {
              updatedMetadata.addPartition(addPartitionDesc.getPartition(0).getPartSpec());
            }
          }
        } else {
          x.getLOG().debug("adding dependent CopyWork/MoveWork for table");
          t.addDependentTask(loadTable(fromURI, table, true, new Path(tblDesc.getLocation()), replicationSpec, x, txnId, stmtId, isSourceMm));
        }
      }
      // Simply create
      x.getTasks().add(t);
    } else {
      // Table existed, and is okay to replicate into, not dropping and re-creating.
      if (table.isPartitioned()) {
        x.getLOG().debug("table partitioned");
        for (AddPartitionDesc addPartitionDesc : partitionDescs) {
          addPartitionDesc.setReplicationSpec(replicationSpec);
          Map<String, String> partSpec = addPartitionDesc.getPartition(0).getPartSpec();
          org.apache.hadoop.hive.ql.metadata.Partition ptn = null;
          Task<?> ict = replicationSpec.isMetadataOnly() ? null : createImportCommitTask(
              tblDesc.getDatabaseName(), tblDesc.getTableName(), txnId, stmtId, x.getConf(),
              AcidUtils.isInsertOnlyTable(tblDesc.getTblProps()));
          if ((ptn = x.getHive().getPartition(table, partSpec, false)) == null) {
            if (!replicationSpec.isMetadataOnly()){
              x.getTasks().add(addSinglePartition(
                  fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec, x, txnId, stmtId, isSourceMm, ict));
              if (updatedMetadata != null) {
                updatedMetadata.addPartition(addPartitionDesc.getPartition(0).getPartSpec());
              }
            } else {
              x.getTasks().add(alterSinglePartition(
                      fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec, null, x));
              if (updatedMetadata != null) {
                updatedMetadata.addPartition(addPartitionDesc.getPartition(0).getPartSpec());
              }
            }
          } else {
            // If replicating, then the partition already existing means we need to replace, maybe, if
            // the destination ptn's repl.last.id is older than the replacement's.
            if (replicationSpec.allowReplacementInto(ptn.getParameters())){
              if (!replicationSpec.isMetadataOnly()){
                x.getTasks().add(addSinglePartition(
                    fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec, x, txnId, stmtId, isSourceMm, ict));
              } else {
                x.getTasks().add(alterSinglePartition(
                    fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec, ptn, x));
              }
              if (updatedMetadata != null) {
                updatedMetadata.addPartition(addPartitionDesc.getPartition(0).getPartSpec());
              }
              if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
                lockType = WriteEntity.WriteType.DDL_SHARED;
              }
            }
          }
        }
        if (replicationSpec.isMetadataOnly() && partitionDescs.isEmpty()){
          // MD-ONLY table alter
          x.getTasks().add(alterTableTask(tblDesc, x,replicationSpec));
          if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
            lockType = WriteEntity.WriteType.DDL_SHARED;
          }
        }
      } else {
        x.getLOG().debug("table non-partitioned");
        if (!replicationSpec.isMetadataOnly()) {
          // repl-imports are replace-into unless the event is insert-into
          loadTable(fromURI, table, replicationSpec.isReplace(), new Path(fromURI),
            replicationSpec, x, txnId, stmtId, isSourceMm);
        } else {
          x.getTasks().add(alterTableTask(tblDesc, x, replicationSpec));
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
