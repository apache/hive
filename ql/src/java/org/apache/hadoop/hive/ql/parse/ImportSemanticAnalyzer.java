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

package org.apache.hadoop.hive.ql.parse;

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
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * ImportSemanticAnalyzer.
 *
 */
public class ImportSemanticAnalyzer extends BaseSemanticAnalyzer {

  public static final String METADATA_NAME="_metadata";

  public ImportSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

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
      boolean isTableSet = false;
      boolean isDbNameSet = false;
      boolean isPartSpecSet = false;
      String parsedLocation = null;
      String parsedTableName = null;
      String parsedDbName = null;
      LinkedHashMap<String, String> parsedPartSpec = new LinkedHashMap<String, String>();

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
            isTableSet = true;
            ASTNode tableNameNode = (ASTNode) child.getChild(0);
            Map.Entry<String,String> dbTablePair = getDbTableNamePair(tableNameNode);
            parsedDbName = dbTablePair.getKey();
            parsedTableName = dbTablePair.getValue();
            if (parsedDbName != null){
              isDbNameSet = true;
            }
            // get partition metadata if partition specified
            if (child.getChildCount() == 2) {
              ASTNode partspec = (ASTNode) child.getChild(1);
              isPartSpecSet = true;
              parsePartitionSpec(child, parsedPartSpec);
            }
            break;
        }
      }

      // parsing statement is now done, on to logic.

      // initialize load path
      URI fromURI = EximUtil.getValidatedURI(conf, stripQuotes(fromTree.getText()));
      FileSystem fs = FileSystem.get(fromURI, conf);
      Path fromPath = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());
      inputs.add(toReadEntity(fromPath));

      EximUtil.ReadMetaData rv = new EximUtil.ReadMetaData();
      try {
        rv =  EximUtil.readMetaData(fs, new Path(fromPath, METADATA_NAME));
      } catch (IOException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }

      ReplicationSpec replicationSpec = rv.getReplicationSpec();
      if (replicationSpec.isNoop()){
        // nothing to do here, silently return.
        return;
      }

      String dbname = SessionState.get().getCurrentDatabase();
      if (isDbNameSet){
        // If the parsed statement contained a db.tablename specification, prefer that.
        dbname = parsedDbName;
      }

      // Create table associated with the import
      // Executed if relevant, and used to contain all the other details about the table if not.
      CreateTableDesc tblDesc = getBaseCreateTableDescFromTable(dbname,rv.getTable());

      if (isExternalSet){
        tblDesc.setExternal(isExternalSet);
        // This condition-check could have been avoided, but to honour the old
        // default of not calling if it wasn't set, we retain that behaviour.
        // TODO:cleanup after verification that the outer if isn't really needed here
      }

      if (isLocationSet){
        tblDesc.setLocation(parsedLocation);
        inputs.add(toReadEntity(parsedLocation));
      }

      if (isTableSet){
        tblDesc.setTableName(parsedTableName);
      }

      List<AddPartitionDesc> partitionDescs = new ArrayList<AddPartitionDesc>();
      Iterable<Partition> partitions = rv.getPartitions();
      for (Partition partition : partitions) {
        // TODO: this should ideally not create AddPartitionDesc per partition
        AddPartitionDesc partsDesc = getBaseAddPartitionDescFromPartition(fromPath, dbname, tblDesc, partition);
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
        conf.set("import.destination.table", tblDesc.getTableName());
        for (AddPartitionDesc addPartitionDesc : partitionDescs) {
          addPartitionDesc.setTableName(tblDesc.getTableName());
        }
      }

      Warehouse wh = new Warehouse(conf);
      Table table = tableIfExists(tblDesc);

      if (table != null){
        checkTable(table, tblDesc,replicationSpec);
        LOG.debug("table " + tblDesc.getTableName() + " exists: metadata checked");
        tableExists = true;
      }

      if (!replicationSpec.isInReplicationScope()){
        createRegularImportTasks(
            rootTasks, tblDesc, partitionDescs,
            isPartSpecSet, replicationSpec, table,
            fromURI, fs, wh);
      } else {
        createReplImportTasks(
            rootTasks, tblDesc, partitionDescs,
            isPartSpecSet, replicationSpec, table,
            fromURI, fs, wh);
      }
    } catch (SemanticException e) {
      throw e;
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg(), e);
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

  private AddPartitionDesc getBaseAddPartitionDescFromPartition(
      Path fromPath, String dbname, CreateTableDesc tblDesc, Partition partition) throws MetaException {
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

  private CreateTableDesc getBaseCreateTableDescFromTable(String dbName,
      org.apache.hadoop.hive.metastore.api.Table table) {
    if ((table.getPartitionKeys() == null) || (table.getPartitionKeys().size() == 0)){
      table.putToParameters(StatsSetupConst.DO_NOT_UPDATE_STATS,"true");
    }
    CreateTableDesc tblDesc = new CreateTableDesc(
        dbName,
        table.getTableName(),
        false, // isExternal: set to false here, can be overwritten by the
               // IMPORT stmt
        table.isTemporary(),
        table.getSd().getCols(),
        table.getPartitionKeys(),
        table.getSd().getBucketCols(),
        table.getSd().getSortCols(),
        table.getSd().getNumBuckets(),
        null, null, null, null, null, // these 5 delims passed as serde params
        null, // comment passed as table params
        table.getSd().getInputFormat(),
        table.getSd().getOutputFormat(),
        null, // location: set to null here, can be
              // overwritten by the IMPORT stmt
        table.getSd().getSerdeInfo().getSerializationLib(),
        null, // storagehandler passed as table params
        table.getSd().getSerdeInfo().getParameters(),
        table.getParameters(), false,
        (null == table.getSd().getSkewedInfo()) ? null : table.getSd().getSkewedInfo()
            .getSkewedColNames(),
        (null == table.getSd().getSkewedInfo()) ? null : table.getSd().getSkewedInfo()
            .getSkewedColValues());
    tblDesc.setStoredAsSubDirectories(table.getSd().isStoredAsSubDirectories());
    return tblDesc;
  }

  private Task<?> loadTable(URI fromURI, Table table, boolean replace) {
    Path dataPath = new Path(fromURI.toString(), "data");
    Path tmpPath = ctx.getExternalTmpPath(new Path(fromURI));
    Task<?> copyTask = TaskFactory.get(new CopyWork(dataPath,
       tmpPath, false), conf);
    LoadTableDesc loadTableWork = new LoadTableDesc(tmpPath,
        Utilities.getTableDesc(table), new TreeMap<String, String>(),
        replace);
    Task<?> loadTableTask = TaskFactory.get(new MoveWork(getInputs(),
        getOutputs(), loadTableWork, null, false), conf);
    copyTask.addDependentTask(loadTableTask);
    rootTasks.add(copyTask);
    return loadTableTask;
  }

  private Task<?> createTableTask(CreateTableDesc tableDesc){
    return TaskFactory.get(new DDLWork(
        getInputs(),
        getOutputs(),
        tableDesc
    ), conf);
  }

  private Task<?> dropTableTask(Table table){
    return TaskFactory.get(new DDLWork(
        getInputs(),
        getOutputs(),
        new DropTableDesc(table.getTableName(), false, true, true, null)
    ), conf);
  }

  private Task<? extends Serializable> alterTableTask(CreateTableDesc tableDesc) {
    tableDesc.setReplaceMode(true);
    return TaskFactory.get(new DDLWork(
        getInputs(),
        getOutputs(),
        tableDesc
    ), conf);
  }

  private Task<? extends Serializable> alterSinglePartition(
      URI fromURI, FileSystem fs, CreateTableDesc tblDesc,
      Table table, Warehouse wh, AddPartitionDesc addPartitionDesc,
      ReplicationSpec replicationSpec, org.apache.hadoop.hive.ql.metadata.Partition ptn) {
    addPartitionDesc.setReplaceMode(true);
    addPartitionDesc.getPartition(0).setLocation(ptn.getLocation()); // use existing location
    return TaskFactory.get(new DDLWork(
        getInputs(),
        getOutputs(),
        addPartitionDesc
    ), conf);
  }


 private Task<?> addSinglePartition(URI fromURI, FileSystem fs, CreateTableDesc tblDesc,
      Table table, Warehouse wh,
      AddPartitionDesc addPartitionDesc, ReplicationSpec replicationSpec)
      throws MetaException, IOException, HiveException {
    AddPartitionDesc.OnePartitionDesc partSpec = addPartitionDesc.getPartition(0);
    if (tblDesc.isExternal() && tblDesc.getLocation() == null) {
      LOG.debug("Importing in-place: adding AddPart for partition "
          + partSpecToString(partSpec.getPartSpec()));
      // addPartitionDesc already has the right partition location
      Task<?> addPartTask = TaskFactory.get(new DDLWork(getInputs(),
          getOutputs(), addPartitionDesc), conf);
      return addPartTask;
    } else {
      String srcLocation = partSpec.getLocation();
      fixLocationInPartSpec(fs, tblDesc, table, wh, replicationSpec, partSpec);
      LOG.debug("adding dependent CopyWork/AddPart/MoveWork for partition "
          + partSpecToString(partSpec.getPartSpec())
          + " with source location: " + srcLocation);
      Path tmpPath = ctx.getExternalTmpPath(new Path(fromURI));
      Task<?> copyTask = TaskFactory.get(new CopyWork(new Path(srcLocation),
          tmpPath, false), conf);
      Task<?> addPartTask = TaskFactory.get(new DDLWork(getInputs(),
          getOutputs(), addPartitionDesc), conf);
      LoadTableDesc loadTableWork = new LoadTableDesc(tmpPath,
          Utilities.getTableDesc(table),
          partSpec.getPartSpec(), true);
      loadTableWork.setInheritTableSpecs(false);
      Task<?> loadPartTask = TaskFactory.get(new MoveWork(
          getInputs(), getOutputs(), loadTableWork, null, false),
          conf);
      copyTask.addDependentTask(loadPartTask);
      addPartTask.addDependentTask(loadPartTask);
      rootTasks.add(copyTask);
      return addPartTask;
    }
  }

  /**
   * Helper method to set location properly in partSpec
   */
  private void fixLocationInPartSpec(
      FileSystem fs, CreateTableDesc tblDesc, Table table,
      Warehouse wh, ReplicationSpec replicationSpec,
      AddPartitionDesc.OnePartitionDesc partSpec) throws MetaException, HiveException, IOException {
    Path tgtPath = null;
    if (tblDesc.getLocation() == null) {
      if (table.getDataLocation() != null) {
        tgtPath = new Path(table.getDataLocation().toString(),
            Warehouse.makePartPath(partSpec.getPartSpec()));
      } else {
        Database parentDb = db.getDatabase(tblDesc.getDatabaseName());
        tgtPath = new Path(
            wh.getTablePath( parentDb, tblDesc.getTableName()),
            Warehouse.makePartPath(partSpec.getPartSpec()));
      }
    } else {
      tgtPath = new Path(tblDesc.getLocation(),
          Warehouse.makePartPath(partSpec.getPartSpec()));
    }
    checkTargetLocationEmpty(fs, tgtPath, replicationSpec);
    partSpec.setLocation(tgtPath.toString());
  }

  private void checkTargetLocationEmpty(FileSystem fs, Path targetPath, ReplicationSpec replicationSpec)
      throws IOException, SemanticException {
    if (replicationSpec.isInReplicationScope()){
      // replication scope allows replacement, and does not require empty directories
      return;
    }
    LOG.debug("checking emptiness of " + targetPath.toString());
    if (fs.exists(targetPath)) {
      FileStatus[] status = fs.listStatus(targetPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      if (status.length > 0) {
        LOG.debug("Files inc. " + status[0].getPath().toString()
            + " found in path : " + targetPath.toString());
        throw new SemanticException(ErrorMsg.TABLE_DATA_EXISTS.getMsg());
      }
    }
  }

  private static String partSpecToString(Map<String, String> partSpec) {
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

  private void checkTable(Table table, CreateTableDesc tableDesc, ReplicationSpec replicationSpec)
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

    // Next, we verify that the destination table is not offline, a view, or a non-native table
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
          throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
            .getMsg());
        }
        importedofc = replaced.getCanonicalName();
      } catch(Exception e) {
        throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
            .getMsg());
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
   */
  private void createRegularImportTasks(
      List<Task<? extends Serializable>> rootTasks,
      CreateTableDesc tblDesc,
      List<AddPartitionDesc> partitionDescs,
      boolean isPartSpecSet,
      ReplicationSpec replicationSpec,
      Table table, URI fromURI, FileSystem fs, Warehouse wh)
      throws HiveException, URISyntaxException, IOException, MetaException {

    if (table != null){
      if (table.isPartitioned()) {
        LOG.debug("table partitioned");

        for (AddPartitionDesc addPartitionDesc : partitionDescs) {
          Map<String, String> partSpec = addPartitionDesc.getPartition(0).getPartSpec();
          org.apache.hadoop.hive.ql.metadata.Partition ptn = null;
          if ((ptn = db.getPartition(table, partSpec, false)) == null) {
            rootTasks.add(addSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec));
          } else {
            throw new SemanticException(
                ErrorMsg.PARTITION_EXISTS.getMsg(partSpecToString(partSpec)));
          }
        }

      } else {
        LOG.debug("table non-partitioned");
        // ensure if destination is not empty only for regular import
        checkTargetLocationEmpty(fs, new Path(table.getDataLocation().toString()), replicationSpec);
        loadTable(fromURI, table, false);
      }
      // Set this to read because we can't overwrite any existing partitions
      outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
    } else {
      LOG.debug("table " + tblDesc.getTableName() + " does not exist");

      Task<?> t = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), tblDesc), conf);
      table = new Table(tblDesc.getDatabaseName(), tblDesc.getTableName());
      Database parentDb = db.getDatabase(tblDesc.getDatabaseName());

      // Since we are going to be creating a new table in a db, we should mark that db as a write entity
      // so that the auth framework can go to work there.
      outputs.add(new WriteEntity(parentDb, WriteEntity.WriteType.DDL_SHARED));

      if (isPartitioned(tblDesc)) {
        for (AddPartitionDesc addPartitionDesc : partitionDescs) {
          t.addDependentTask(
              addSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec));
        }
      } else {
        LOG.debug("adding dependent CopyWork/MoveWork for table");
        if (tblDesc.isExternal() && (tblDesc.getLocation() == null)) {
          LOG.debug("Importing in place, no emptiness check, no copying/loading");
          Path dataPath = new Path(fromURI.toString(), "data");
          tblDesc.setLocation(dataPath.toString());
        } else {
          Path tablePath = null;
          if (tblDesc.getLocation() != null) {
            tablePath = new Path(tblDesc.getLocation());
          } else {
            tablePath = wh.getTablePath(parentDb, tblDesc.getTableName());
          }
          checkTargetLocationEmpty(fs, tablePath, replicationSpec);
          t.addDependentTask(loadTable(fromURI, table, false));
        }
      }
      rootTasks.add(t);
    }
  }

  /**
   * Create tasks for repl import
   */
  private void createReplImportTasks(
      List<Task<? extends Serializable>> rootTasks,
      CreateTableDesc tblDesc,
      List<AddPartitionDesc> partitionDescs,
      boolean isPartSpecSet, ReplicationSpec replicationSpec, Table table, URI fromURI, FileSystem fs, Warehouse wh)
      throws HiveException, URISyntaxException, IOException, MetaException {

    Task dr = null;
    WriteEntity.WriteType lockType = WriteEntity.WriteType.DDL_NO_LOCK;

    if ((table != null) && (isPartitioned(tblDesc) != table.isPartitioned())){
      // If destination table exists, but is partitioned, and we think we're writing to an unpartitioned
      // or if destination table exists, but is unpartitioned and we think we're writing to a partitioned
      // table, then this can only happen because there are drops in the queue that are yet to be processed.
      // So, we check the repl.last.id of the destination, and if it's newer, we no-op. If it's older, we
      // drop and re-create.
      if (replicationSpec.allowReplacementInto(table)){
        dr = dropTableTask(table);
        lockType = WriteEntity.WriteType.DDL_EXCLUSIVE;
        table = null; // null it out so we go into the table re-create flow.
      } else {
        return; // noop out of here.
      }
    }

    Database parentDb = db.getDatabase(tblDesc.getDatabaseName());
    if (parentDb == null){
      throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(tblDesc.getDatabaseName()));
    }
    if (tblDesc.getLocation() == null) {
      tblDesc.setLocation(wh.getTablePath(parentDb, tblDesc.getTableName()).toString());
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
      // Either we're dropping and re-creating, or the table didn't exist, and we're creating.

      if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
        lockType = WriteEntity.WriteType.DDL_SHARED;
      }

      Task t = createTableTask(tblDesc);
      table = new Table(tblDesc.getDatabaseName(), tblDesc.getTableName());

      if (!replicationSpec.isMetadataOnly()) {
        if (isPartitioned(tblDesc)) {
          for (AddPartitionDesc addPartitionDesc : partitionDescs) {
            t.addDependentTask(
                addSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec));
          }
        } else {
          LOG.debug("adding dependent CopyWork/MoveWork for table");
          t.addDependentTask(loadTable(fromURI, table, true));
        }
      }
      if (dr == null){
        // Simply create
        rootTasks.add(t);
      } else {
        // Drop and recreate
        dr.addDependentTask(t);
        rootTasks.add(dr);
      }
    } else {
      // Table existed, and is okay to replicate into, not dropping and re-creating.
      if (table.isPartitioned()) {
        LOG.debug("table partitioned");
        for (AddPartitionDesc addPartitionDesc : partitionDescs) {

          Map<String, String> partSpec = addPartitionDesc.getPartition(0).getPartSpec();
          org.apache.hadoop.hive.ql.metadata.Partition ptn = null;

          if ((ptn = db.getPartition(table, partSpec, false)) == null) {
            if (!replicationSpec.isMetadataOnly()){
              rootTasks.add(addSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec));
            }
          } else {
            // If replicating, then the partition already existing means we need to replace, maybe, if
            // the destination ptn's repl.last.id is older than the replacement's.
            if (replicationSpec.allowReplacementInto(ptn)){
              if (!replicationSpec.isMetadataOnly()){
                rootTasks.add(addSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec));
              } else {
                rootTasks.add(alterSinglePartition(fromURI, fs, tblDesc, table, wh, addPartitionDesc, replicationSpec, ptn));
              }
              if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
                lockType = WriteEntity.WriteType.DDL_SHARED;
              }
            } else {
              // ignore this ptn, do nothing, not an error.
            }
          }

        }
        if (replicationSpec.isMetadataOnly() && partitionDescs.isEmpty()){
          // MD-ONLY table alter
          rootTasks.add(alterTableTask(tblDesc));
          if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
            lockType = WriteEntity.WriteType.DDL_SHARED;
          }
        }
      } else {
        LOG.debug("table non-partitioned");
        if (!replicationSpec.allowReplacementInto(table)){
          return; // silently return, table is newer than our replacement.
        }
        if (!replicationSpec.isMetadataOnly()) {
          loadTable(fromURI, table, true); // repl-imports are replace-into
        } else {
          rootTasks.add(alterTableTask(tblDesc));
        }
        if (lockType == WriteEntity.WriteType.DDL_NO_LOCK){
          lockType = WriteEntity.WriteType.DDL_SHARED;
        }
      }
    }
    outputs.add(new WriteEntity(table,lockType));

  }

  private boolean isPartitioned(CreateTableDesc tblDesc) {
    return !(tblDesc.getPartCols() == null || tblDesc.getPartCols().isEmpty());
  }

  /**
   * Utility method that returns a table if one corresponding to the destination
   * tblDesc is found. Returns null if no such table is found.
   */
   private Table tableIfExists(CreateTableDesc tblDesc) throws HiveException {
    try {
      return db.getTable(tblDesc.getDatabaseName(),tblDesc.getTableName());
    } catch (InvalidTableException e) {
      return null;
    }
  }

}
