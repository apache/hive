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

package org.apache.hadoop.hive.ql.exec;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.Msck;
import org.apache.hadoop.hive.metastore.MsckInfo;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils.PartSpecInfo;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileTask;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatter;
import org.apache.hadoop.hive.ql.metadata.formatting.TextMetaDataTable;
import org.apache.hadoop.hive.ql.parse.AlterTablePartMergeFilesDesc;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableAlterPartDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.AlterTableExchangePartition;
import org.apache.hadoop.hive.ql.plan.AlterTableSimpleDesc;
import org.apache.hadoop.hive.ql.plan.CacheMetadataDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DropPartitionDesc;
import org.apache.hadoop.hive.ql.plan.FileMergeDesc;
import org.apache.hadoop.hive.ql.plan.InsertCommitHookDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.OrcFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.RCFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.RenamePartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReplRemoveFirstIncLoadPendFlagDesc;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
import org.apache.hadoop.hive.ql.plan.ShowConfDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DDLTask implementation.
 *
 **/
public class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.DDLTask");

  private static final int separator = Utilities.tabCode;
  private static final int terminator = Utilities.newLineCode;

  // These are suffixes attached to intermediate directory names used in the
  // archiving / un-archiving process.
  private static String INTERMEDIATE_ARCHIVED_DIR_SUFFIX;
  private static String INTERMEDIATE_ORIGINAL_DIR_SUFFIX;
  private static String INTERMEDIATE_EXTRACTED_DIR_SUFFIX;

  private MetaDataFormatter formatter;

  @Override
  public boolean requireLock() {
    return this.work != null && this.work.getNeedLock();
  }

  public DDLTask() {
    super();
  }

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, DriverContext ctx,
      CompilationOpContext opContext) {
    super.initialize(queryState, queryPlan, ctx, opContext);

    // Pick the formatter to use to display the results.  Either the
    // normal human readable output or a json object.
    formatter = MetaDataFormatUtils.getFormatter(conf);
    INTERMEDIATE_ARCHIVED_DIR_SUFFIX =
        HiveConf.getVar(conf, ConfVars.METASTORE_INT_ARCHIVED);
    INTERMEDIATE_ORIGINAL_DIR_SUFFIX =
        HiveConf.getVar(conf, ConfVars.METASTORE_INT_ORIGINAL);
    INTERMEDIATE_EXTRACTED_DIR_SUFFIX =
        HiveConf.getVar(conf, ConfVars.METASTORE_INT_EXTRACTED);
  }

  @Override
  public int execute(DriverContext driverContext) {
    if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }

    // Create the db
    Hive db;
    try {
      db = Hive.get(conf);

      DropPartitionDesc dropPartition = work.getDropPartitionDesc();
      if (dropPartition != null) {
        dropPartitions(db, dropPartition);
        return 0;
      }

      AlterTableDesc alterTbl = work.getAlterTblDesc();
      if (alterTbl != null) {
        if (!allowOperationInReplicationScope(db, alterTbl.getOldName(), null, alterTbl.getReplicationSpec())) {
          // no alter, the table is missing either due to drop/rename which follows the alter.
          // or the existing table is newer than our update.
          LOG.debug("DDLTask: Alter Table is skipped as table {} is newer than update", alterTbl.getOldName());
          return 0;
        }
        if (alterTbl.getOp() == AlterTableTypes.DROPCONSTRAINT ) {
          return dropConstraint(db, alterTbl);
        } else if (alterTbl.getOp() == AlterTableTypes.ADDCONSTRAINT) {
          return addConstraints(db, alterTbl);
        } else {
          return alterTable(db, alterTbl);
        }
      }

      AddPartitionDesc addPartitionDesc = work.getAddPartitionDesc();
      if (addPartitionDesc != null) {
        return addPartitions(db, addPartitionDesc);
      }

      RenamePartitionDesc renamePartitionDesc = work.getRenamePartitionDesc();
      if (renamePartitionDesc != null) {
        return renamePartition(db, renamePartitionDesc);
      }

      AlterTableSimpleDesc simpleDesc = work.getAlterTblSimpleDesc();
      if (simpleDesc != null) {
        if (simpleDesc.getType() == AlterTableTypes.TOUCH) {
          return touch(db, simpleDesc);
        } else if (simpleDesc.getType() == AlterTableTypes.ARCHIVE) {
          return archive(db, simpleDesc, driverContext);
        } else if (simpleDesc.getType() == AlterTableTypes.UNARCHIVE) {
          return unarchive(db, simpleDesc);
        } else if (simpleDesc.getType() == AlterTableTypes.COMPACT) {
          return compact(db, simpleDesc);
        }
      }

      MsckDesc msckDesc = work.getMsckDesc();
      if (msckDesc != null) {
        return msck(db, msckDesc);
      }

      ShowColumnsDesc showCols = work.getShowColumnsDesc();
      if (showCols != null) {
        return showColumns(db, showCols);
      }

      ShowPartitionsDesc showParts = work.getShowPartsDesc();
      if (showParts != null) {
        return showPartitions(db, showParts);
      }

      ShowConfDesc showConf = work.getShowConfDesc();
      if (showConf != null) {
        return showConf(db, showConf);
      }

      AlterTablePartMergeFilesDesc mergeFilesDesc = work.getMergeFilesDesc();
      if (mergeFilesDesc != null) {
        return mergeFiles(db, mergeFilesDesc, driverContext);
      }

      AlterTableAlterPartDesc alterPartDesc = work.getAlterTableAlterPartDesc();
      if(alterPartDesc != null) {
        return alterTableAlterPart(db, alterPartDesc);
      }

      AlterTableExchangePartition alterTableExchangePartition =
          work.getAlterTableExchangePartition();
      if (alterTableExchangePartition != null) {
        return exchangeTablePartition(db, alterTableExchangePartition);
      }

      CacheMetadataDesc cacheMetadataDesc = work.getCacheMetadataDesc();
      if (cacheMetadataDesc != null) {
        return cacheMetadata(db, cacheMetadataDesc);
      }
      InsertCommitHookDesc insertCommitHookDesc = work.getInsertCommitHookDesc();
      if (insertCommitHookDesc != null) {
        return insertCommitWork(db, insertCommitHookDesc);
      }

      if (work.getReplSetFirstIncLoadFlagDesc() != null) {
        return remFirstIncPendFlag(db, work.getReplSetFirstIncLoadFlagDesc());
      }
    } catch (Throwable e) {
      failed(e);
      return 1;
    }
    assert false;
    return 0;
  }

  private int insertCommitWork(Hive db, InsertCommitHookDesc insertCommitHookDesc) throws MetaException {
    boolean failed = true;
    HiveMetaHook hook = insertCommitHookDesc.getTable().getStorageHandler().getMetaHook();
    if (hook == null || !(hook instanceof DefaultHiveMetaHook)) {
      return 0;
    }
    DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;
    try {
      hiveMetaHook.commitInsertTable(insertCommitHookDesc.getTable().getTTable(),
              insertCommitHookDesc.isOverwrite()
      );
      failed = false;
    } finally {
      if (failed) {
        hiveMetaHook.rollbackInsertTable(insertCommitHookDesc.getTable().getTTable(),
                insertCommitHookDesc.isOverwrite()
        );
      }
    }
    return 0;
  }

  private int cacheMetadata(Hive db, CacheMetadataDesc desc) throws HiveException {
    db.cacheFileMetadata(desc.getDbName(), desc.getTableName(),
        desc.getPartName(), desc.isAllParts());
    return 0;
  }

  private void failed(Throwable e) {
    while (e.getCause() != null && e.getClass() == RuntimeException.class) {
      e = e.getCause();
    }
    setException(e);
    LOG.error("Failed", e);
  }

  private int showConf(Hive db, ShowConfDesc showConf) throws Exception {
    ConfVars conf = HiveConf.getConfVars(showConf.getConfName());
    if (conf == null) {
      throw new HiveException("invalid configuration name " + showConf.getConfName());
    }
    String description = conf.getDescription();
    String defaultValue = conf.getDefaultValue();
    DataOutputStream output = getOutputStream(showConf.getResFile());
    try {
      if (defaultValue != null) {
        output.write(defaultValue.getBytes());
      }
      output.write(separator);
      output.write(conf.typeString().getBytes());
      output.write(separator);
      if (description != null) {
        output.write(description.replaceAll(" *\n *", " ").getBytes());
      }
      output.write(terminator);
    } finally {
      output.close();
    }
    return 0;
  }

  private DataOutputStream getOutputStream(String resFile) throws HiveException {
    try {
      return getOutputStream(new Path(resFile));
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private DataOutputStream getOutputStream(Path outputFile) throws HiveException {
    try {
      FileSystem fs = outputFile.getFileSystem(conf);
      return fs.create(outputFile);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * First, make sure the source table/partition is not
   * archived/indexes/non-rcfile. If either of these is true, throw an
   * exception.
   *
   * The way how it does the merge is to create a BlockMergeTask from the
   * mergeFilesDesc.
   *
   * @param db
   * @param mergeFilesDesc
   * @return
   * @throws HiveException
   */
  private int mergeFiles(Hive db, AlterTablePartMergeFilesDesc mergeFilesDesc,
      DriverContext driverContext) throws HiveException {
    ListBucketingCtx lbCtx = mergeFilesDesc.getLbCtx();
    boolean lbatc = lbCtx == null ? false : lbCtx.isSkewedStoredAsDir();
    int lbd = lbCtx == null ? 0 : lbCtx.calculateListBucketingLevel();

    // merge work only needs input and output.
    MergeFileWork mergeWork = new MergeFileWork(mergeFilesDesc.getInputDir(),
        mergeFilesDesc.getOutputDir(), mergeFilesDesc.getInputFormatClass().getName(),
        mergeFilesDesc.getTableDesc());
    LinkedHashMap<Path, ArrayList<String>> pathToAliases = new LinkedHashMap<>();
    ArrayList<String> inputDirstr = new ArrayList<String>(1);
    inputDirstr.add(mergeFilesDesc.getInputDir().toString());
    pathToAliases.put(mergeFilesDesc.getInputDir().get(0), inputDirstr);
    mergeWork.setPathToAliases(pathToAliases);
    mergeWork.setListBucketingCtx(mergeFilesDesc.getLbCtx());
    mergeWork.resolveConcatenateMerge(db.getConf());
    mergeWork.setMapperCannotSpanPartns(true);
    mergeWork.setSourceTableInputFormat(mergeFilesDesc.getInputFormatClass().getName());
    final FileMergeDesc fmd;
    if (mergeFilesDesc.getInputFormatClass().equals(RCFileInputFormat.class)) {
      fmd = new RCFileMergeDesc();
    } else {
      // safe to assume else is ORC as semantic analyzer will check for RC/ORC
      fmd = new OrcFileMergeDesc();
    }

    fmd.setDpCtx(null);
    fmd.setHasDynamicPartitions(false);
    fmd.setListBucketingAlterTableConcatenate(lbatc);
    fmd.setListBucketingDepth(lbd);
    fmd.setOutputPath(mergeFilesDesc.getOutputDir());

    CompilationOpContext opContext = driverContext.getCtx().getOpContext();
    Operator<? extends OperatorDesc> mergeOp = OperatorFactory.get(opContext, fmd);

    LinkedHashMap<String, Operator<? extends  OperatorDesc>> aliasToWork =
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
    aliasToWork.put(mergeFilesDesc.getInputDir().toString(), mergeOp);
    mergeWork.setAliasToWork(aliasToWork);
    DriverContext driverCxt = new DriverContext();
    Task<?> task;
    if (conf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      TezWork tezWork = new TezWork(queryState.getQueryId(), conf);
      mergeWork.setName("File Merge");
      tezWork.add(mergeWork);
      task = new TezTask();
      ((TezTask) task).setWork(tezWork);
    } else {
      task = new MergeFileTask();
      ((MergeFileTask) task).setWork(mergeWork);
    }

    // initialize the task and execute
    task.initialize(queryState, getQueryPlan(), driverCxt, opContext);
    Task<? extends Serializable> subtask = task;
    int ret = task.execute(driverCxt);
    if (subtask.getException() != null) {
      setException(subtask.getException());
    }
    return ret;
  }

  /**
   * Add a partitions to a table.
   *
   * @param db
   *          Database to add the partition to.
   * @param addPartitionDesc
   *          Add these partitions.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int addPartitions(Hive db, AddPartitionDesc addPartitionDesc) throws HiveException {
    List<Partition> parts = db.createPartitions(addPartitionDesc);
    for (Partition part : parts) {
      addIfAbsentByName(new WriteEntity(part, WriteEntity.WriteType.INSERT));
    }
    return 0;
  }

  /**
   * Rename a partition in a table
   *
   * @param db
   *          Database to rename the partition.
   * @param renamePartitionDesc
   *          rename old Partition to new one.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int renamePartition(Hive db, RenamePartitionDesc renamePartitionDesc) throws HiveException {
    String tableName = renamePartitionDesc.getTableName();
    LinkedHashMap<String, String> oldPartSpec = renamePartitionDesc.getOldPartSpec();

    if (!allowOperationInReplicationScope(db, tableName, oldPartSpec, renamePartitionDesc.getReplicationSpec())) {
      // no rename, the table is missing either due to drop/rename which follows the current rename.
      // or the existing table is newer than our update.
      if (LOG.isDebugEnabled()) {
      LOG.debug("DDLTask: Rename Partition is skipped as table {} / partition {} is newer than update",
              tableName,
              FileUtils.makePartName(new ArrayList<>(oldPartSpec.keySet()), new ArrayList<>(oldPartSpec.values())));
      }
      return 0;
    }

    String names[] = Utilities.getDbTableName(tableName);
    if (Utils.isBootstrapDumpInProgress(db, names[0])) {
      LOG.error("DDLTask: Rename Partition not allowed as bootstrap dump in progress");
      throw new HiveException("Rename Partition: Not allowed as bootstrap dump in progress");
    }

    Table tbl = db.getTable(tableName);
    Partition oldPart = db.getPartition(tbl, oldPartSpec, false);
    if (oldPart == null) {
      String partName = FileUtils.makePartName(new ArrayList<String>(oldPartSpec.keySet()),
          new ArrayList<String>(oldPartSpec.values()));
      throw new HiveException("Rename partition: source partition [" + partName
          + "] does not exist.");
    }
    Partition part = db.getPartition(tbl, oldPartSpec, false);
    part.setValues(renamePartitionDesc.getNewPartSpec());
    long writeId = renamePartitionDesc.getWriteId();
    if (renamePartitionDesc.getReplicationSpec() != null
            && renamePartitionDesc.getReplicationSpec().isMigratingToTxnTable()) {
      Long tmpWriteId = ReplUtils.getMigrationCurrentTblWriteId(conf);
      if (tmpWriteId == null) {
        throw new HiveException("DDLTask : Write id is not set in the config by open txn task for migration");
      }
      writeId = tmpWriteId;
    }
    db.renamePartition(tbl, oldPartSpec, part, writeId);
    Partition newPart = db.getPartition(tbl, renamePartitionDesc.getNewPartSpec(), false);
    work.getInputs().add(new ReadEntity(oldPart));
    // We've already obtained a lock on the table, don't lock the partition too
    addIfAbsentByName(new WriteEntity(newPart, WriteEntity.WriteType.DDL_NO_LOCK));
    return 0;
  }

  /**
   * Alter partition column type in a table
   *
   * @param db
   *          Database to rename the partition.
   * @param alterPartitionDesc
   *          change partition column type.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   */
  private int alterTableAlterPart(Hive db, AlterTableAlterPartDesc alterPartitionDesc)
      throws HiveException {

    Table tbl = db.getTable(alterPartitionDesc.getTableName(), true);

    // This is checked by DDLSemanticAnalyzer
    assert(tbl.isPartitioned());

    List<FieldSchema> newPartitionKeys = new ArrayList<FieldSchema>();

    //Check if the existing partition values can be type casted to the new column type
    // with a non null value before trying to alter the partition column type.
    try {
      Set<Partition> partitions = db.getAllPartitionsOf(tbl);
      int colIndex = -1;
      for(FieldSchema col : tbl.getTTable().getPartitionKeys()) {
        colIndex++;
        if (col.getName().compareTo(alterPartitionDesc.getPartKeySpec().getName()) == 0) {
          break;
        }
      }

      if (colIndex == -1 || colIndex == tbl.getTTable().getPartitionKeys().size()) {
        throw new HiveException("Cannot find partition column " +
            alterPartitionDesc.getPartKeySpec().getName());
      }

      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(alterPartitionDesc.getPartKeySpec().getType());
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(expectedType);
      Converter converter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);

      // For all the existing partitions, check if the value can be type casted to a non-null object
      for(Partition part : partitions) {
        if (part.getName().equals(conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME))) {
          continue;
        }
        try {
          String value = part.getValues().get(colIndex);
          Object convertedValue =
              converter.convert(value);
          if (convertedValue == null) {
            throw new HiveException(" Converting from " + TypeInfoFactory.stringTypeInfo + " to " +
              expectedType + " for value : " + value + " resulted in NULL object");
          }
        } catch (Exception e) {
          throw new HiveException("Exception while converting " +
              TypeInfoFactory.stringTypeInfo + " to " +
              expectedType + " for value : " + part.getValues().get(colIndex));
        }
      }
    } catch(Exception e) {
      throw new HiveException(
          "Exception while checking type conversion of existing partition values to " +
          alterPartitionDesc.getPartKeySpec() + " : " + e.getMessage());
    }

    for(FieldSchema col : tbl.getTTable().getPartitionKeys()) {
      if (col.getName().compareTo(alterPartitionDesc.getPartKeySpec().getName()) == 0) {
        newPartitionKeys.add(alterPartitionDesc.getPartKeySpec());
      } else {
        newPartitionKeys.add(col);
      }
    }

    tbl.getTTable().setPartitionKeys(newPartitionKeys);

    db.alterTable(tbl, false, null, true);

    work.getInputs().add(new ReadEntity(tbl));
    // We've already locked the table as the input, don't relock it as the output.
    addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));

    return 0;
  }

  /**
   * Rewrite the partition's metadata and force the pre/post execute hooks to
   * be fired.
   *
   * @param db
   * @param touchDesc
   * @return
   * @throws HiveException
   */
  private int touch(Hive db, AlterTableSimpleDesc touchDesc)
      throws HiveException {
    // TODO: catalog
    Table tbl = db.getTable(touchDesc.getTableName());
    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);

    if (touchDesc.getPartSpec() == null) {
      db.alterTable(tbl, false, environmentContext, true);
      work.getInputs().add(new ReadEntity(tbl));
      addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    } else {
      Partition part = db.getPartition(tbl, touchDesc.getPartSpec(), false);
      if (part == null) {
        throw new HiveException("Specified partition does not exist");
      }
      try {
        db.alterPartition(tbl.getCatalogName(), tbl.getDbName(), tbl.getTableName(),
            part, environmentContext, true);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
      work.getInputs().add(new ReadEntity(part));
      addIfAbsentByName(new WriteEntity(part, WriteEntity.WriteType.DDL_NO_LOCK));
    }
    return 0;
  }

  /**
   * Sets archiving flag locally; it has to be pushed into metastore
   * @param p partition to set flag
   * @param state desired state of IS_ARCHIVED flag
   * @param level desired level for state == true, anything for false
   */
  private void setIsArchived(Partition p, boolean state, int level) {
    Map<String, String> params = p.getParameters();
    if (state) {
      params.put(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.IS_ARCHIVED,
          "true");
      params.put(ArchiveUtils.ARCHIVING_LEVEL, Integer
          .toString(level));
    } else {
      params.remove(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.IS_ARCHIVED);
      params.remove(ArchiveUtils.ARCHIVING_LEVEL);
    }
  }

  /**
   * Returns original partition of archived partition, null for unarchived one
   */
  private String getOriginalLocation(Partition p) {
    Map<String, String> params = p.getParameters();
    return params.get(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ORIGINAL_LOCATION);
  }

  /**
   * Sets original location of partition which is to be archived
   */
  private void setOriginalLocation(Partition p, String loc) {
    Map<String, String> params = p.getParameters();
    if (loc == null) {
      params.remove(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ORIGINAL_LOCATION);
    } else {
      params.put(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ORIGINAL_LOCATION, loc);
    }
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark
   * it as archived. Note that the metastore is not touched - a separate
   * call to alter_partition is needed.
   *
   * @param p - the partition object to modify
   * @param harPath - new location of partition (har schema URI)
   */
  private void setArchived(Partition p, Path harPath, int level) {
    assert(ArchiveUtils.isArchived(p) == false);
    setIsArchived(p, true, level);
    setOriginalLocation(p, p.getLocation());
    p.setLocation(harPath.toString());
  }

  /**
   * Sets the appropriate attributes in the supplied Partition object to mark
   * it as not archived. Note that the metastore is not touched - a separate
   * call to alter_partition is needed.
   *
   * @param p - the partition to modify
   */
  private void setUnArchived(Partition p) {
    assert(ArchiveUtils.isArchived(p) == true);
    String parentDir = getOriginalLocation(p);
    setIsArchived(p, false, 0);
    setOriginalLocation(p, null);
    assert(parentDir != null);
    p.setLocation(parentDir);
  }

  private boolean pathExists(Path p) throws HiveException {
    try {
      FileSystem fs = p.getFileSystem(conf);
      return fs.exists(p);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void moveDir(FileSystem fs, Path from, Path to) throws HiveException {
    try {
      if (!fs.rename(from, to)) {
        throw new HiveException("Moving " + from + " to " + to + " failed!");
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void deleteDir(Path dir, Database db) throws HiveException {
    try {
      Warehouse wh = new Warehouse(conf);
      wh.deleteDir(dir, true, db);
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Checks in partition is in custom (not-standard) location.
   * @param tbl - table in which partition is
   * @param p - partition
   * @return true if partition location is custom, false if it is standard
   */
  boolean partitionInCustomLocation(Table tbl, Partition p)
      throws HiveException {
    String subdir = null;
    try {
      subdir = Warehouse.makePartName(tbl.getPartCols(), p.getValues());
    } catch (MetaException e) {
      throw new HiveException("Unable to get partition's directory", e);
    }
    Path tableDir = tbl.getDataLocation();
    if(tableDir == null) {
      throw new HiveException("Table has no location set");
    }

    String standardLocation = (new Path(tableDir, subdir)).toString();
    if(ArchiveUtils.isArchived(p)) {
      return !getOriginalLocation(p).equals(standardLocation);
    } else {
      return !p.getLocation().equals(standardLocation);
    }
  }

  private int archive(Hive db, AlterTableSimpleDesc simpleDesc,
      DriverContext driverContext)
          throws HiveException {

    Table tbl = db.getTable(simpleDesc.getTableName());

    if (tbl.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("ARCHIVE can only be performed on managed tables");
    }

    Map<String, String> partSpec = simpleDesc.getPartSpec();
    PartSpecInfo partSpecInfo = PartSpecInfo.create(tbl, partSpec);
    List<Partition> partitions = db.getPartitions(tbl, partSpec);

    Path originalDir = null;

    // when we have partial partitions specification we must assume partitions
    // lie in standard place - if they were in custom locations putting
    // them into one archive would involve mass amount of copying
    // in full partition specification case we allow custom locations
    // to keep backward compatibility
    if (partitions.isEmpty()) {
      throw new HiveException("No partition matches the specification");
    } else if(partSpecInfo.values.size() != tbl.getPartCols().size()) {
      // for partial specifications we need partitions to follow the scheme
      for(Partition p: partitions){
        if(partitionInCustomLocation(tbl, p)) {
          String message = String.format("ARCHIVE cannot run for partition " +
              "groups with custom locations like %s", p.getLocation());
          throw new HiveException(message);
        }
      }
      originalDir = partSpecInfo.createPath(tbl);
    } else {
      Partition p = partitions.get(0);
      // partition can be archived if during recovery
      if(ArchiveUtils.isArchived(p)) {
        originalDir = new Path(getOriginalLocation(p));
      } else {
        originalDir = p.getDataLocation();
      }
    }

    Path intermediateArchivedDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ARCHIVED_DIR_SUFFIX);
    Path intermediateOriginalDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ORIGINAL_DIR_SUFFIX);

    console.printInfo("intermediate.archived is " + intermediateArchivedDir.toString());
    console.printInfo("intermediate.original is " + intermediateOriginalDir.toString());

    String archiveName = "data.har";
    FileSystem fs = null;
    try {
      fs = originalDir.getFileSystem(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    URI archiveUri = (new Path(originalDir, archiveName)).toUri();
    URI originalUri = ArchiveUtils.addSlash(originalDir.toUri());
    ArchiveUtils.HarPathHelper harHelper = new ArchiveUtils.HarPathHelper(
        conf, archiveUri, originalUri);

    // we checked if partitions matching specification are marked as archived
    // in the metadata; if they are and their levels are the same as we would
    // set it later it means previous run failed and we have to do the recovery;
    // if they are different, we throw an error
    for(Partition p: partitions) {
      if(ArchiveUtils.isArchived(p)) {
        if(ArchiveUtils.getArchivingLevel(p) != partSpecInfo.values.size()) {
          String name = ArchiveUtils.getPartialName(p, ArchiveUtils.getArchivingLevel(p));
          String m = String.format("Conflict with existing archive %s", name);
          throw new HiveException(m);
        } else {
          throw new HiveException("Partition(s) already archived");
        }
      }
    }

    boolean recovery = false;
    if (pathExists(intermediateArchivedDir)
        || pathExists(intermediateOriginalDir)) {
      recovery = true;
      console.printInfo("Starting recovery after failed ARCHIVE");
    }

    // The following steps seem roundabout, but they are meant to aid in
    // recovery if a failure occurs and to keep a consistent state in the FS

    // Steps:
    // 1. Create the archive in a temporary folder
    // 2. Move the archive dir to an intermediate dir that is in at the same
    //    dir as the original partition dir. Call the new dir
    //    intermediate-archive.
    // 3. Rename the original partition dir to an intermediate dir. Call the
    //    renamed dir intermediate-original
    // 4. Rename intermediate-archive to the original partition dir
    // 5. Change the metadata
    // 6. Delete the original partition files in intermediate-original

    // The original partition files are deleted after the metadata change
    // because the presence of those files are used to indicate whether
    // the original partition directory contains archived or unarchived files.

    // Create an archived version of the partition in a directory ending in
    // ARCHIVE_INTERMEDIATE_DIR_SUFFIX that's the same level as the partition,
    // if it does not already exist. If it does exist, we assume the dir is good
    // to use as the move operation that created it is atomic.
    if (!pathExists(intermediateArchivedDir) &&
        !pathExists(intermediateOriginalDir)) {

      // First create the archive in a tmp dir so that if the job fails, the
      // bad files don't pollute the filesystem
      Path tmpPath = new Path(driverContext.getCtx()
          .getExternalTmpPath(originalDir), "partlevel");

      console.printInfo("Creating " + archiveName +
          " for " + originalDir.toString());
      console.printInfo("in " + tmpPath);
      console.printInfo("Please wait... (this may take a while)");

      // Create the Hadoop archive
      int ret=0;
      try {
        int maxJobNameLen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
        String jobname = String.format("Archiving %s@%s",
            tbl.getTableName(), partSpecInfo.getName());
        jobname = Utilities.abbreviate(jobname, maxJobNameLen - 6);
        conf.set(MRJobConfig.JOB_NAME, jobname);
        HadoopArchives har = new HadoopArchives(conf);
        List<String> args = new ArrayList<String>();

        args.add("-archiveName");
        args.add(archiveName);
        args.add("-p");
        args.add(originalDir.toString());
        args.add(tmpPath.toString());

        ret = ToolRunner.run(har, args.toArray(new String[0]));
      } catch (Exception e) {
        throw new HiveException(e);
      }
      if (ret != 0) {
        throw new HiveException("Error while creating HAR");
      }

      // Move from the tmp dir to an intermediate directory, in the same level as
      // the partition directory. e.g. .../hr=12-intermediate-archived
      try {
        console.printInfo("Moving " + tmpPath + " to " + intermediateArchivedDir);
        if (pathExists(intermediateArchivedDir)) {
          throw new HiveException("The intermediate archive directory already exists.");
        }
        fs.rename(tmpPath, intermediateArchivedDir);
      } catch (IOException e) {
        throw new HiveException("Error while moving tmp directory");
      }
    } else {
      if (pathExists(intermediateArchivedDir)) {
        console.printInfo("Intermediate archive directory " + intermediateArchivedDir +
            " already exists. Assuming it contains an archived version of the partition");
      }
    }

    // If we get to here, we know that we've archived the partition files, but
    // they may be in the original partition location, or in the intermediate
    // original dir.

    // Move the original parent directory to the intermediate original directory
    // if the move hasn't been made already
    if (!pathExists(intermediateOriginalDir)) {
      console.printInfo("Moving " + originalDir + " to " +
          intermediateOriginalDir);
      moveDir(fs, originalDir, intermediateOriginalDir);
    } else {
      console.printInfo(intermediateOriginalDir + " already exists. " +
          "Assuming it contains the original files in the partition");
    }

    // If there's a failure from here to when the metadata is updated,
    // there will be no data in the partition, or an error while trying to read
    // the partition (if the archive files have been moved to the original
    // partition directory.) But re-running the archive command will allow
    // recovery

    // Move the intermediate archived directory to the original parent directory
    if (!pathExists(originalDir)) {
      console.printInfo("Moving " + intermediateArchivedDir + " to " +
          originalDir);
      moveDir(fs, intermediateArchivedDir, originalDir);
    } else {
      console.printInfo(originalDir + " already exists. " +
          "Assuming it contains the archived version of the partition");
    }

    // Record this change in the metastore
    try {
      for(Partition p: partitions) {
        URI originalPartitionUri = ArchiveUtils.addSlash(p.getDataLocation().toUri());
        URI harPartitionDir = harHelper.getHarUri(originalPartitionUri);
        StringBuilder authority = new StringBuilder();
        if(harPartitionDir.getUserInfo() != null) {
          authority.append(harPartitionDir.getUserInfo()).append("@");
        }
        authority.append(harPartitionDir.getHost());
        if(harPartitionDir.getPort() != -1) {
          authority.append(":").append(harPartitionDir.getPort());
        }
        Path harPath = new Path(harPartitionDir.getScheme(),
            authority.toString(),
            harPartitionDir.getPath()); // make in Path to ensure no slash at the end
        setArchived(p, harPath, partSpecInfo.values.size());
        // TODO: catalog
        db.alterPartition(simpleDesc.getTableName(), p, null, true);
      }
    } catch (Exception e) {
      throw new HiveException("Unable to change the partition info for HAR", e);
    }

    // If a failure occurs here, the directory containing the original files
    // will not be deleted. The user will run ARCHIVE again to clear this up
    if(pathExists(intermediateOriginalDir)) {
      deleteDir(intermediateOriginalDir, db.getDatabase(tbl.getDbName()));
    }

    if(recovery) {
      console.printInfo("Recovery after ARCHIVE succeeded");
    }

    return 0;
  }

  private int unarchive(Hive db, AlterTableSimpleDesc simpleDesc)
      throws HiveException, URISyntaxException {

    Table tbl = db.getTable(simpleDesc.getTableName());

    // Means user specified a table, not a partition
    if (simpleDesc.getPartSpec() == null) {
      throw new HiveException("UNARCHIVE is for partitions only");
    }

    if (tbl.getTableType() != TableType.MANAGED_TABLE) {
      throw new HiveException("UNARCHIVE can only be performed on managed tables");
    }

    Map<String, String> partSpec = simpleDesc.getPartSpec();
    PartSpecInfo partSpecInfo = PartSpecInfo.create(tbl, partSpec);
    List<Partition> partitions = db.getPartitions(tbl, partSpec);

    int partSpecLevel = partSpec.size();

    Path originalDir = null;

    // when we have partial partitions specification we must assume partitions
    // lie in standard place - if they were in custom locations putting
    // them into one archive would involve mass amount of copying
    // in full partition specification case we allow custom locations
    // to keep backward compatibility
    if (partitions.isEmpty()) {
      throw new HiveException("No partition matches the specification");
    } else if(partSpecInfo.values.size() != tbl.getPartCols().size()) {
      // for partial specifications we need partitions to follow the scheme
      for(Partition p: partitions){
        if(partitionInCustomLocation(tbl, p)) {
          String message = String.format("UNARCHIVE cannot run for partition " +
              "groups with custom locations like %s", p.getLocation());
          throw new HiveException(message);
        }
      }
      originalDir = partSpecInfo.createPath(tbl);
    } else {
      Partition p = partitions.get(0);
      if(ArchiveUtils.isArchived(p)) {
        originalDir = new Path(getOriginalLocation(p));
      } else {
        originalDir = new Path(p.getLocation());
      }
    }

    URI originalUri = ArchiveUtils.addSlash(originalDir.toUri());
    Path intermediateArchivedDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_ARCHIVED_DIR_SUFFIX);
    Path intermediateExtractedDir = new Path(originalDir.getParent(),
        originalDir.getName() + INTERMEDIATE_EXTRACTED_DIR_SUFFIX);
    boolean recovery = false;
    if(pathExists(intermediateArchivedDir) || pathExists(intermediateExtractedDir)) {
      recovery = true;
      console.printInfo("Starting recovery after failed UNARCHIVE");
    }

    for(Partition p: partitions) {
      checkArchiveProperty(partSpecLevel, recovery, p);
    }

    String archiveName = "data.har";
    FileSystem fs = null;
    try {
      fs = originalDir.getFileSystem(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    // assume the archive is in the original dir, check if it exists
    Path archivePath = new Path(originalDir, archiveName);
    URI archiveUri = archivePath.toUri();
    ArchiveUtils.HarPathHelper harHelper = new ArchiveUtils.HarPathHelper(conf,
        archiveUri, originalUri);
    URI sourceUri = harHelper.getHarUri(originalUri);
    Path sourceDir = new Path(sourceUri.getScheme(), sourceUri.getAuthority(), sourceUri.getPath());

    if(!pathExists(intermediateArchivedDir) && !pathExists(archivePath)) {
      throw new HiveException("Haven't found any archive where it should be");
    }

    Path tmpPath = driverContext.getCtx().getExternalTmpPath(originalDir);

    try {
      fs = tmpPath.getFileSystem(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    // Clarification of terms:
    // - The originalDir directory represents the original directory of the
    //   partitions' files. They now contain an archived version of those files
    //   eg. hdfs:/warehouse/myTable/ds=1/
    // - The source directory is the directory containing all the files that
    //   should be in the partitions. e.g. har:/warehouse/myTable/ds=1/myTable.har/
    //   Note the har:/ scheme

    // Steps:
    // 1. Extract the archive in a temporary folder
    // 2. Move the archive dir to an intermediate dir that is in at the same
    //    dir as originalLocation. Call the new dir intermediate-extracted.
    // 3. Rename the original partitions dir to an intermediate dir. Call the
    //    renamed dir intermediate-archive
    // 4. Rename intermediate-extracted to the original partitions dir
    // 5. Change the metadata
    // 6. Delete the archived partitions files in intermediate-archive

    if (!pathExists(intermediateExtractedDir) &&
        !pathExists(intermediateArchivedDir)) {
      try {

        // Copy the files out of the archive into the temporary directory
        String copySource = sourceDir.toString();
        String copyDest = tmpPath.toString();
        List<String> args = new ArrayList<String>();
        args.add("-cp");
        args.add(copySource);
        args.add(copyDest);

        console.printInfo("Copying " + copySource + " to " + copyDest);
        FileSystem srcFs = FileSystem.get(sourceDir.toUri(), conf);
        srcFs.initialize(sourceDir.toUri(), conf);

        FsShell fss = new FsShell(conf);
        int ret = 0;
        try {
          ret = ToolRunner.run(fss, args.toArray(new String[0]));
        } catch (Exception e) {
          throw new HiveException(e);
        }

        if (ret != 0) {
          throw new HiveException("Error while copying files from archive, return code=" + ret);
        } else {
          console.printInfo("Successfully Copied " + copySource + " to " + copyDest);
        }

        console.printInfo("Moving " + tmpPath + " to " + intermediateExtractedDir);
        if (fs.exists(intermediateExtractedDir)) {
          throw new HiveException("Invalid state: the intermediate extracted " +
              "directory already exists.");
        }
        fs.rename(tmpPath, intermediateExtractedDir);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }

    // At this point, we know that the extracted files are in the intermediate
    // extracted dir, or in the the original directory.

    if (!pathExists(intermediateArchivedDir)) {
      try {
        console.printInfo("Moving " + originalDir + " to " + intermediateArchivedDir);
        fs.rename(originalDir, intermediateArchivedDir);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      console.printInfo(intermediateArchivedDir + " already exists. " +
          "Assuming it contains the archived version of the partition");
    }

    // If there is a failure from here to until when the metadata is changed,
    // the partition will be empty or throw errors on read.

    // If the original location exists here, then it must be the extracted files
    // because in the previous step, we moved the previous original location
    // (containing the archived version of the files) to intermediateArchiveDir
    if (!pathExists(originalDir)) {
      try {
        console.printInfo("Moving " + intermediateExtractedDir + " to " + originalDir);
        fs.rename(intermediateExtractedDir, originalDir);
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      console.printInfo(originalDir + " already exists. " +
          "Assuming it contains the extracted files in the partition");
    }

    for(Partition p: partitions) {
      setUnArchived(p);
      try {
        // TODO: catalog
        db.alterPartition(simpleDesc.getTableName(), p, null, true);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
    }

    // If a failure happens here, the intermediate archive files won't be
    // deleted. The user will need to call unarchive again to clear those up.
    if(pathExists(intermediateArchivedDir)) {
      deleteDir(intermediateArchivedDir, db.getDatabase(tbl.getDbName()));
    }

    if(recovery) {
      console.printInfo("Recovery after UNARCHIVE succeeded");
    }

    return 0;
  }

  private void checkArchiveProperty(int partSpecLevel,
      boolean recovery, Partition p) throws HiveException {
    if (!ArchiveUtils.isArchived(p) && !recovery) {
      throw new HiveException("Partition " + p.getName()
          + " is not archived.");
    }
    int archiveLevel = ArchiveUtils.getArchivingLevel(p);
    if (partSpecLevel > archiveLevel) {
      throw new HiveException("Partition " + p.getName()
          + " is archived at level " + archiveLevel
          + ", and given partspec only has " + partSpecLevel
          + " specs.");
    }
  }

  private int compact(Hive db, AlterTableSimpleDesc desc) throws HiveException {

    Table tbl = db.getTable(desc.getTableName());
    if (!AcidUtils.isTransactionalTable(tbl)) {
      throw new HiveException(ErrorMsg.NONACID_COMPACTION_NOT_SUPPORTED, tbl.getDbName(),
          tbl.getTableName());
    }

    String partName = null;
    if (desc.getPartSpec() == null) {
      // Compaction can only be done on the whole table if the table is non-partitioned.
      if (tbl.isPartitioned()) {
        throw new HiveException(ErrorMsg.NO_COMPACTION_PARTITION);
      }
    } else {
      Map<String, String> partSpec = desc.getPartSpec();
      List<Partition> partitions = db.getPartitions(tbl, partSpec);
      if (partitions.size() > 1) {
        throw new HiveException(ErrorMsg.TOO_MANY_COMPACTION_PARTITIONS);
      } else if (partitions.size() == 0) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION_SPEC);
      }
      partName = partitions.get(0).getName();
    }
    CompactionResponse resp = db.compact2(tbl.getDbName(), tbl.getTableName(), partName,
      desc.getCompactionType(), desc.getProps());
    if(resp.isAccepted()) {
      console.printInfo("Compaction enqueued with id " + resp.getId());
    }
    else {
      console.printInfo("Compaction already enqueued with id " + resp.getId() +
        "; State is " + resp.getState());
    }
    if(desc.isBlocking() && resp.isAccepted()) {
      StringBuilder progressDots = new StringBuilder();
      long waitTimeMs = 1000;
      wait: while (true) {
        //double wait time until 5min
        waitTimeMs = waitTimeMs*2;
        waitTimeMs = waitTimeMs < 5*60*1000 ? waitTimeMs : 5*60*1000;
        try {
          Thread.sleep(waitTimeMs);
        }
        catch(InterruptedException ex) {
          console.printInfo("Interrupted while waiting for compaction with id=" + resp.getId());
          break;
        }
        //this could be expensive when there are a lot of compactions....
        //todo: update to search by ID once HIVE-13353 is done
        ShowCompactResponse allCompactions = db.showCompactions();
        for(ShowCompactResponseElement compaction : allCompactions.getCompacts()) {
          if (resp.getId() != compaction.getId()) {
            continue;
          }
          switch (compaction.getState()) {
            case TxnStore.WORKING_RESPONSE:
            case TxnStore.INITIATED_RESPONSE:
              //still working
              console.printInfo(progressDots.toString());
              progressDots.append(".");
              continue wait;
            default:
              //done
              console.printInfo("Compaction with id " + resp.getId() + " finished with status: " + compaction.getState());
              break wait;
          }
        }
      }
    }
    return 0;
  }

  /**
   * MetastoreCheck, see if the data in the metastore matches what is on the
   * dfs. Current version checks for tables and partitions that are either
   * missing on disk on in the metastore.
   *
   * @param db
   *          The database in question.
   * @param msckDesc
   *          Information about the tables and partitions we want to check for.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   */
  private int msck(Hive db, MsckDesc msckDesc) {
    Msck msck;
    try {
      msck = new Msck( false, false);
      msck.init(db.getConf());
      String[] names = Utilities.getDbTableName(msckDesc.getTableName());
      MsckInfo msckInfo = new MsckInfo(SessionState.get().getCurrentCatalog(), names[0],
        names[1], msckDesc.getPartSpecs(), msckDesc.getResFile(),
        msckDesc.isRepairPartitions(), msckDesc.isAddPartitions(), msckDesc.isDropPartitions(), -1);
      return msck.repair(msckInfo);
    } catch (MetaException e) {
      LOG.error("Unable to create msck instance.", e);
      return 1;
    } catch (SemanticException e) {
      LOG.error("Msck failed.", e);
      return 1;
    }
  }

  /**
   * Write a list of partitions to a file.
   *
   * @param db
   *          The database in question.
   * @param showParts
   *          These are the partitions we're interested in.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int showPartitions(Hive db, ShowPartitionsDesc showParts) throws HiveException {
    // get the partitions for the table and populate the output
    String tabName = showParts.getTabName();
    Table tbl = null;
    List<String> parts = null;

    tbl = db.getTable(tabName);

    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tabName);
    }
    if (showParts.getPartSpec() != null) {
      parts = db.getPartitionNames(tbl.getDbName(),
          tbl.getTableName(), showParts.getPartSpec(), (short) -1);
    } else {
      parts = db.getPartitionNames(tbl.getDbName(), tbl.getTableName(), (short) -1);
    }

    // write the results in the file
    DataOutputStream outStream = getOutputStream(showParts.getResFile());
    try {
      formatter.showTablePartitions(outStream, parts);
    } catch (Exception e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, "show partitions for table " + tabName);
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
  }

  /**
   * Write a list of the columns in the table to a file.
   *
   * @param db
   *          The database in context.
   * @param showCols
   *        A ShowColumnsDesc for columns we're interested in.
   * @return Returns 0 when execution succeeds.
   * @throws HiveException
   *        Throws this exception if an unexpected error occurs.
   */
  public int showColumns(Hive db, ShowColumnsDesc showCols)
      throws HiveException {

    Table table = db.getTable(showCols.getTableName());

    // write the results in the file
    DataOutputStream outStream = getOutputStream(showCols.getResFile());
    try {
      List<FieldSchema> allCols = table.getCols();
      allCols.addAll(table.getPartCols());
      List<FieldSchema> cols = getColumnsByPattern(allCols,showCols.getPattern());
      // In case the query is served by HiveServer2, don't pad it with spaces,
      // as HiveServer2 output is consumed by JDBC/ODBC clients.
      boolean isOutputPadded = !SessionState.get().isHiveServerQuery();
      TextMetaDataTable tmd = new TextMetaDataTable();
      for (FieldSchema fieldSchema : cols) {
        tmd.addRow(MetaDataFormatUtils.extractColumnValues(fieldSchema));
      }
      outStream.writeBytes(tmd.renderTable(isOutputPadded));
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  /**
   * Returns a sorted list of columns matching a column pattern.
   *
   * @param cols
   *        Columns of a table.
   * @param columnPattern
   *        we want to find columns similar to a column pattern.
   * @return sorted list of columns.
   */
  private List<FieldSchema> getColumnsByPattern(List<FieldSchema> cols, String columnPattern) {

    if(columnPattern == null) {
      columnPattern = "*";
    }
    columnPattern = columnPattern.toLowerCase();
    columnPattern = columnPattern.replaceAll("\\*", ".*");
    Pattern pattern = Pattern.compile(columnPattern);
    Matcher matcher = pattern.matcher("");

    SortedSet<FieldSchema> sortedCol = new TreeSet<>( new Comparator<FieldSchema>() {
      @Override
      public int compare(FieldSchema f1, FieldSchema f2) {
        return f1.getName().compareTo(f2.getName());
      }
    });

    for(FieldSchema column : cols)  {
      matcher.reset(column.getName());
      if(matcher.matches()) {
        sortedCol.add(column);
      }
    }

    return new ArrayList<FieldSchema>(sortedCol);
  }

  /**
   * Alter a given table.
   *
   * @param db
   *          The database in question.
   * @param alterTbl
   *          This is the table we're altering.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private int alterTable(Hive db, AlterTableDesc alterTbl) throws HiveException {
    if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAME) {
      String names[] = Utilities.getDbTableName(alterTbl.getOldName());
      if (Utils.isBootstrapDumpInProgress(db, names[0])) {
        LOG.error("DDLTask: Rename Table not allowed as bootstrap dump in progress");
        throw new HiveException("Rename Table: Not allowed as bootstrap dump in progress");
      }
    }

    // alter the table
    Table tbl = db.getTable(alterTbl.getOldName());

    List<Partition> allPartitions = null;
    if (alterTbl.getPartSpec() != null) {
      Map<String, String> partSpec = alterTbl.getPartSpec();
      if (DDLSemanticAnalyzer.isFullSpec(tbl, partSpec)) {
        allPartitions = new ArrayList<Partition>();
        Partition part = db.getPartition(tbl, partSpec, false);
        if (part == null) {
          // User provided a fully specified partition spec but it doesn't exist, fail.
          throw new HiveException(ErrorMsg.INVALID_PARTITION,
                StringUtils.join(alterTbl.getPartSpec().keySet(), ',') + " for table " + alterTbl.getOldName());

        }
        allPartitions.add(part);
      } else {
        // DDLSemanticAnalyzer has already checked if partial partition specs are allowed,
        // thus we should not need to check it here.
        allPartitions = db.getPartitions(tbl, alterTbl.getPartSpec());
      }
    }

    // Don't change the table object returned by the metastore, as we'll mess with it's caches.
    Table oldTbl = tbl;
    tbl = oldTbl.copy();
    // Handle child tasks here. We could add them directly whereever we need,
    // but let's make it a little bit more explicit.
    if (allPartitions != null) {
      // Alter all partitions
      for (Partition part : allPartitions) {
        addChildTasks(alterTableOrSinglePartition(alterTbl, tbl, part));
      }
    } else {
      // Just alter the table
      addChildTasks(alterTableOrSinglePartition(alterTbl, tbl, null));
    }

    if (allPartitions == null) {
      updateModifiedParameters(tbl.getTTable().getParameters(), conf);
      tbl.checkValidity(conf);
    } else {
      for (Partition tmpPart: allPartitions) {
        updateModifiedParameters(tmpPart.getParameters(), conf);
      }
    }

    try {
      EnvironmentContext environmentContext = alterTbl.getEnvironmentContext();
      if (environmentContext == null) {
        environmentContext = new EnvironmentContext();
      }
      environmentContext.putToProperties(HiveMetaHook.ALTER_TABLE_OPERATION_TYPE, alterTbl.getOp().name());
      if (allPartitions == null) {
        long writeId = alterTbl.getWriteId() != null ? alterTbl.getWriteId() : 0;
        if (alterTbl.getReplicationSpec() != null &&
                alterTbl.getReplicationSpec().isMigratingToTxnTable()) {
          Long tmpWriteId = ReplUtils.getMigrationCurrentTblWriteId(conf);
          if (tmpWriteId == null) {
            throw new HiveException("DDLTask : Write id is not set in the config by open txn task for migration");
          }
          writeId = tmpWriteId;
        }
        db.alterTable(alterTbl.getOldName(), tbl, alterTbl.getIsCascade(), environmentContext,
                true, writeId);
      } else {
        // Note: this is necessary for UPDATE_STATISTICS command, that operates via ADDPROPS (why?).
        //       For any other updates, we don't want to do txn check on partitions when altering table.
        boolean isTxn = false;
        if (alterTbl.getPartSpec() != null && alterTbl.getOp() == AlterTableTypes.ADDPROPS) {
          // ADDPROPS is used to add replication properties like repl.last.id, which isn't
          // transactional change. In case of replication check for transactional properties
          // explicitly.
          Map<String, String> props = alterTbl.getProps();
          if (alterTbl.getReplicationSpec() != null && alterTbl.getReplicationSpec().isInReplicationScope()) {
            isTxn = (props.get(StatsSetupConst.COLUMN_STATS_ACCURATE) != null);
          } else {
            isTxn = true;
          }
        }
        db.alterPartitions(Warehouse.getQualifiedName(tbl.getTTable()), allPartitions, environmentContext, isTxn);
      }
      // Add constraints if necessary
      addConstraints(db, alterTbl);
    } catch (InvalidOperationException e) {
      LOG.error("alter table: ", e);
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    // This is kind of hacky - the read entity contains the old table, whereas
    // the write entity
    // contains the new table. This is needed for rename - both the old and the
    // new table names are
    // passed
    // Don't acquire locks for any of these, we have already asked for them in DDLSemanticAnalyzer.
    if (allPartitions != null ) {
      for (Partition tmpPart: allPartitions) {
        work.getInputs().add(new ReadEntity(tmpPart));
        addIfAbsentByName(new WriteEntity(tmpPart, WriteEntity.WriteType.DDL_NO_LOCK));
      }
    } else {
      work.getInputs().add(new ReadEntity(oldTbl));
      addIfAbsentByName(new WriteEntity(tbl, WriteEntity.WriteType.DDL_NO_LOCK));
    }
    return 0;
  }
  /**
   * There are many places where "duplicate" Read/WriteEnity objects are added.  The way this was
   * initially implemented, the duplicate just replaced the previous object.
   * (work.getOutputs() is a Set and WriteEntity#equals() relies on name)
   * This may be benign for ReadEntity and perhaps was benign for WriteEntity before WriteType was
   * added. Now that WriteEntity has a WriteType it replaces it with one with possibly different
   * {@link org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType}.  It's hard to imagine
   * how this is desirable.
   *
   * As of HIVE-14993, WriteEntity with different WriteType must be considered different.
   * So WriteEntity created in DDLTask cause extra output in golden files, but only because
   * DDLTask sets a different WriteType for the same Entity.
   *
   * In the spirit of bug-for-bug compatibility, this method ensures we only add new
   * WriteEntity if it's really new.
   *
   * @return {@code true} if item was added
   */
  static boolean addIfAbsentByName(WriteEntity newWriteEntity, Set<WriteEntity> outputs) {
    for(WriteEntity writeEntity : outputs) {
      if(writeEntity.getName().equalsIgnoreCase(newWriteEntity.getName())) {
        LOG.debug("Ignoring request to add {} because {} is present",
          newWriteEntity.toStringDetail(), writeEntity.toStringDetail());
        return false;
      }
    }
    outputs.add(newWriteEntity);
    return true;
  }
  private boolean addIfAbsentByName(WriteEntity newWriteEntity) {
    return addIfAbsentByName(newWriteEntity, work.getOutputs());
  }

  private void addChildTasks(List<Task<?>> extraTasks) {
    if (extraTasks == null) {
      return;
    }
    for (Task<?> newTask : extraTasks) {
      addDependentTask(newTask);
    }
  }

  private boolean isSchemaEvolutionEnabled(Table tbl) {
    boolean isAcid = AcidUtils.isTablePropertyTransactional(tbl.getMetadata());
    if (isAcid || HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION)) {
      return true;
    }
    return false;
  }


  private static StorageDescriptor retrieveStorageDescriptor(Table tbl, Partition part) {
    return (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
  }

  private List<Task<?>> alterTableOrSinglePartition(AlterTableDesc alterTbl, Table tbl,
                                                    Partition part) throws HiveException {
    EnvironmentContext environmentContext = alterTbl.getEnvironmentContext();
    if (environmentContext == null) {
      environmentContext = new EnvironmentContext();
      alterTbl.setEnvironmentContext(environmentContext);
    }
    // do not need update stats in alter table/partition operations
    if (environmentContext.getProperties() == null ||
        environmentContext.getProperties().get(StatsSetupConst.DO_NOT_UPDATE_STATS) == null) {
      environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }

    if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAME) {
      tbl.setDbName(Utilities.getDatabaseName(alterTbl.getNewName()));
      tbl.setTableName(Utilities.getTableName(alterTbl.getNewName()));
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCOLS) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      String serializationLib = sd.getSerdeInfo().getSerializationLib();
      AvroSerdeUtils.handleAlterTableForAvro(conf, serializationLib, tbl.getTTable().getParameters());
      List<FieldSchema> oldCols = (part == null
          ? tbl.getColsForMetastore() : part.getColsForMetastore());
      List<FieldSchema> newCols = alterTbl.getNewCols();
      if (serializationLib.equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
        .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
        sd.setCols(newCols);
      } else {
        // make sure the columns does not already exist
        Iterator<FieldSchema> iterNewCols = newCols.iterator();
        while (iterNewCols.hasNext()) {
          FieldSchema newCol = iterNewCols.next();
          String newColName = newCol.getName();
          Iterator<FieldSchema> iterOldCols = oldCols.iterator();
          while (iterOldCols.hasNext()) {
            String oldColName = iterOldCols.next().getName();
            if (oldColName.equalsIgnoreCase(newColName)) {
              throw new HiveException(ErrorMsg.DUPLICATE_COLUMN_NAMES, newColName);
            }
          }
          oldCols.add(newCol);
        }
        sd.setCols(oldCols);
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.RENAMECOLUMN) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      String serializationLib = sd.getSerdeInfo().getSerializationLib();
      AvroSerdeUtils.handleAlterTableForAvro(conf, serializationLib, tbl.getTTable().getParameters());
      List<FieldSchema> oldCols = (part == null
          ? tbl.getColsForMetastore() : part.getColsForMetastore());
      List<FieldSchema> newCols = new ArrayList<FieldSchema>();
      Iterator<FieldSchema> iterOldCols = oldCols.iterator();
      String oldName = alterTbl.getOldColName();
      String newName = alterTbl.getNewColName();
      String type = alterTbl.getNewColType();
      String comment = alterTbl.getNewColComment();
      boolean first = alterTbl.getFirst();
      String afterCol = alterTbl.getAfterCol();
      // if orc table, restrict reordering columns as it will break schema evolution
      boolean isOrcSchemaEvolution =
          sd.getInputFormat().equals(OrcInputFormat.class.getName()) &&
          isSchemaEvolutionEnabled(tbl);
      if (isOrcSchemaEvolution && (first || (afterCol != null && !afterCol.trim().isEmpty()))) {
        throw new HiveException(ErrorMsg.CANNOT_REORDER_COLUMNS, alterTbl.getOldName());
      }
      FieldSchema column = null;

      boolean found = false;
      int position = -1;
      if (first) {
        position = 0;
      }

      int i = 1;
      while (iterOldCols.hasNext()) {
        FieldSchema col = iterOldCols.next();
        String oldColName = col.getName();
        if (oldColName.equalsIgnoreCase(newName)
            && !oldColName.equalsIgnoreCase(oldName)) {
          throw new HiveException(ErrorMsg.DUPLICATE_COLUMN_NAMES, newName);
        } else if (oldColName.equalsIgnoreCase(oldName)) {
          col.setName(newName);
          if (type != null && !type.trim().equals("")) {
            col.setType(type);
          }
          if (comment != null) {
            col.setComment(comment);
          }
          found = true;
          if (first || (afterCol != null && !afterCol.trim().equals(""))) {
            column = col;
            continue;
          }
        }

        if (afterCol != null && !afterCol.trim().equals("")
            && oldColName.equalsIgnoreCase(afterCol)) {
          position = i;
        }

        i++;
        newCols.add(col);
      }

      // did not find the column
      if (!found) {
        throw new HiveException(ErrorMsg.INVALID_COLUMN, oldName);
      }
      // after column is not null, but we did not find it.
      if ((afterCol != null && !afterCol.trim().equals("")) && position < 0) {
        throw new HiveException(ErrorMsg.INVALID_COLUMN, afterCol);
      }

      if (position >= 0) {
        newCols.add(position, column);
      }

      sd.setCols(newCols);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.REPLACECOLS) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      // change SerDe to LazySimpleSerDe if it is columnsetSerDe
      String serializationLib = sd.getSerdeInfo().getSerializationLib();
      if (serializationLib.equals(
          "org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        console
        .printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      } else if (!serializationLib.equals(
          MetadataTypedColumnsetSerDe.class.getName())
          && !serializationLib.equals(LazySimpleSerDe.class.getName())
          && !serializationLib.equals(ColumnarSerDe.class.getName())
          && !serializationLib.equals(DynamicSerDe.class.getName())
          && !serializationLib.equals(ParquetHiveSerDe.class.getName())
          && !serializationLib.equals(OrcSerde.class.getName())) {
        throw new HiveException(ErrorMsg.CANNOT_REPLACE_COLUMNS, alterTbl.getOldName());
      }
      final boolean isOrcSchemaEvolution =
          serializationLib.equals(OrcSerde.class.getName()) &&
          isSchemaEvolutionEnabled(tbl);
      // adding columns and limited integer type promotion is supported for ORC schema evolution
      if (isOrcSchemaEvolution) {
        final List<FieldSchema> existingCols = sd.getCols();
        final List<FieldSchema> replaceCols = alterTbl.getNewCols();

        if (replaceCols.size() < existingCols.size()) {
          throw new HiveException(ErrorMsg.REPLACE_CANNOT_DROP_COLUMNS, alterTbl.getOldName());
        }
      }

      boolean partitioned = tbl.isPartitioned();
      boolean droppingColumns = alterTbl.getNewCols().size() < sd.getCols().size();
      if (ParquetHiveSerDe.isParquetTable(tbl) &&
          isSchemaEvolutionEnabled(tbl) &&
          !alterTbl.getIsCascade() &&
          droppingColumns && partitioned) {
        LOG.warn("Cannot drop columns from a partitioned parquet table without the CASCADE option");
        throw new HiveException(ErrorMsg.REPLACE_CANNOT_DROP_COLUMNS,
            alterTbl.getOldName());
      }
      sd.setCols(alterTbl.getNewCols());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDPROPS) {
      return alterTableAddProps(alterTbl, tbl, part, environmentContext);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.DROPPROPS) {
      return alterTableDropProps(alterTbl, tbl, part, environmentContext);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDEPROPS) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      sd.getSerdeInfo().getParameters().putAll(alterTbl.getProps());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSERDE) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      String serdeName = alterTbl.getSerdeName();
      String oldSerdeName = sd.getSerdeInfo().getSerializationLib();
      // if orc table, restrict changing the serde as it can break schema evolution
      if (isSchemaEvolutionEnabled(tbl) &&
          oldSerdeName.equalsIgnoreCase(OrcSerde.class.getName()) &&
          !serdeName.equalsIgnoreCase(OrcSerde.class.getName())) {
        throw new HiveException(ErrorMsg.CANNOT_CHANGE_SERDE, OrcSerde.class.getSimpleName(),
            alterTbl.getOldName());
      }
      sd.getSerdeInfo().setSerializationLib(serdeName);
      if ((alterTbl.getProps() != null) && (alterTbl.getProps().size() > 0)) {
        sd.getSerdeInfo().getParameters().putAll(alterTbl.getProps());
      }
      if (part != null) {
        // TODO: wtf? This doesn't do anything.
        part.getTPartition().getSd().setCols(part.getTPartition().getSd().getCols());
      } else {
        if (Table.shouldStoreFieldsInMetastore(conf, serdeName, tbl.getParameters())
            && !Table.hasMetastoreBasedSchema(conf, oldSerdeName)) {
          // If new SerDe needs to store fields in metastore, but the old serde doesn't, save
          // the fields so that new SerDe could operate. Note that this may fail if some fields
          // from old SerDe are too long to be stored in metastore, but there's nothing we can do.
          try {
            Deserializer oldSerde = HiveMetaStoreUtils.getDeserializer(
                conf, tbl.getTTable(), false, oldSerdeName);
            tbl.setFields(Hive.getFieldsFromDeserializer(tbl.getTableName(), oldSerde));
          } catch (MetaException ex) {
            throw new HiveException(ex);
          }
        }
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDFILEFORMAT) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      // if orc table, restrict changing the file format as it can break schema evolution
      if (isSchemaEvolutionEnabled(tbl) &&
          sd.getInputFormat().equals(OrcInputFormat.class.getName())
          && !alterTbl.getInputFormat().equals(OrcInputFormat.class.getName())) {
        throw new HiveException(ErrorMsg.CANNOT_CHANGE_FILEFORMAT, "ORC", alterTbl.getOldName());
      }
      sd.setInputFormat(alterTbl.getInputFormat());
      sd.setOutputFormat(alterTbl.getOutputFormat());
      if (alterTbl.getSerdeName() != null) {
        sd.getSerdeInfo().setSerializationLib(alterTbl.getSerdeName());
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDCLUSTERSORTCOLUMN) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      // validate sort columns and bucket columns
      List<String> columns = Utilities.getColumnNamesFromFieldSchema(tbl
          .getCols());
      if (!alterTbl.isTurnOffSorting()) {
        Utilities.validateColumnNames(columns, alterTbl.getBucketColumns());
      }
      if (alterTbl.getSortColumns() != null) {
        Utilities.validateColumnNames(columns, Utilities
            .getColumnNamesFromSortCols(alterTbl.getSortColumns()));
      }

      if (alterTbl.isTurnOffSorting()) {
        sd.setSortCols(new ArrayList<Order>());
      } else if (alterTbl.getNumberBuckets() == -1) {
        // -1 buckets means to turn off bucketing
        sd.setBucketCols(new ArrayList<String>());
        sd.setNumBuckets(-1);
        sd.setSortCols(new ArrayList<Order>());
      } else {
        sd.setBucketCols(alterTbl.getBucketColumns());
        sd.setNumBuckets(alterTbl.getNumberBuckets());
        sd.setSortCols(alterTbl.getSortColumns());
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ALTERLOCATION) {
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      String newLocation = alterTbl.getNewLocation();
      try {
        URI locUri = new URI(newLocation);
        if (!new Path(locUri).isAbsolute()) {
          throw new HiveException(ErrorMsg.BAD_LOCATION_VALUE, newLocation);
        }
        sd.setLocation(newLocation);
      } catch (URISyntaxException e) {
        throw new HiveException(e);
      }
      environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);

    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDSKEWEDBY) {
      // Validation's been done at compile time. no validation is needed here.
      List<String> skewedColNames = null;
      List<List<String>> skewedValues = null;

      if (alterTbl.isTurnOffSkewed()) {
        // Convert skewed table to non-skewed table.
        skewedColNames = new ArrayList<String>();
        skewedValues = new ArrayList<List<String>>();
      } else {
        skewedColNames = alterTbl.getSkewedColNames();
        skewedValues = alterTbl.getSkewedColValues();
      }

      if ( null == tbl.getSkewedInfo()) {
        // Convert non-skewed table to skewed table.
        SkewedInfo skewedInfo = new SkewedInfo();
        skewedInfo.setSkewedColNames(skewedColNames);
        skewedInfo.setSkewedColValues(skewedValues);
        tbl.setSkewedInfo(skewedInfo);
      } else {
        tbl.setSkewedColNames(skewedColNames);
        tbl.setSkewedColValues(skewedValues);
      }

      tbl.setStoredAsSubDirectories(alterTbl.isStoredAsSubDirectories());
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.OWNER) {
      if (alterTbl.getOwnerPrincipal() != null) {
        tbl.setOwner(alterTbl.getOwnerPrincipal().getName());
        tbl.setOwnerType(alterTbl.getOwnerPrincipal().getType());
      }
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ALTERSKEWEDLOCATION) {
      // process location one-by-one
      Map<List<String>,String> locMaps = alterTbl.getSkewedLocations();
      Set<List<String>> keys = locMaps.keySet();
      for(List<String> key:keys){
        String newLocation = locMaps.get(key);
        try {
          URI locUri = new URI(newLocation);
          if (part != null) {
            List<String> slk = new ArrayList<String>(key);
            part.setSkewedValueLocationMap(slk, locUri.toString());
          } else {
            List<String> slk = new ArrayList<String>(key);
            tbl.setSkewedValueLocationMap(slk, locUri.toString());
          }
        } catch (URISyntaxException e) {
          throw new HiveException(e);
        }
      }

      environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
    } else if (alterTbl.getOp() == AlterTableTypes.ALTERBUCKETNUM) {
      if (part != null) {
        if (part.getBucketCount() == alterTbl.getNumberBuckets()) {
          return null;
        }
        part.setBucketCount(alterTbl.getNumberBuckets());
      } else {
        if (tbl.getNumBuckets() == alterTbl.getNumberBuckets()) {
          return null;
        }
        tbl.setNumBuckets(alterTbl.getNumberBuckets());
      }
    } else if (alterTbl.getOp() == AlterTableTypes.UPDATECOLUMNS) {
      updateColumns(tbl, part);
    } else {
      throw new HiveException(ErrorMsg.UNSUPPORTED_ALTER_TBL_OP, alterTbl.getOp().toString());
    }

    return null;
  }

  private List<Task<?>> alterTableDropProps(AlterTableDesc alterTbl, Table tbl,
      Partition part, EnvironmentContext environmentContext) throws HiveException {
    if (StatsSetupConst.USER.equals(environmentContext.getProperties()
        .get(StatsSetupConst.STATS_GENERATED))) {
      // drop a stats parameter, which triggers recompute stats update automatically
      environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
    }

    List<Task<?>> result = null;
    if (part == null) {
      Set<String> removedSet = alterTbl.getProps().keySet();
      boolean isFromMmTable = AcidUtils.isInsertOnlyTable(tbl.getParameters()),
          isRemoved = AcidUtils.isRemovedInsertOnlyTable(removedSet);
      if (isFromMmTable && isRemoved) {
        throw new HiveException("Cannot convert an ACID table to non-ACID");
      }

      // Check if external table property being removed
      if (removedSet.contains("EXTERNAL") && tbl.getTableType() == TableType.EXTERNAL_TABLE) {
        tbl.setTableType(TableType.MANAGED_TABLE);
      }
    }
    Iterator<String> keyItr = alterTbl.getProps().keySet().iterator();
    while (keyItr.hasNext()) {
      if (part != null) {
        part.getTPartition().getParameters().remove(keyItr.next());
      } else {
        tbl.getTTable().getParameters().remove(keyItr.next());
      }
    }
    return result;
  }

  private void checkMmLb(Table tbl) throws HiveException {
    if (!tbl.isStoredAsSubDirectories()) {
      return;
    }
    // TODO [MM gap?]: by design; no-one seems to use LB tables. They will work, but not convert.
    //                 It's possible to work around this by re-creating and re-inserting the table.
    throw new HiveException("Converting list bucketed tables stored as subdirectories "
        + " to MM is not supported. Please re-create a table in the desired format.");
  }

  private void checkMmLb(Partition part) throws HiveException {
    if (!part.isStoredAsSubDirectories()) {
      return;
    }
    throw new HiveException("Converting list bucketed tables stored as subdirectories "
        + " to MM is not supported. Please re-create a table in the desired format.");
  }

  private List<Task<?>> generateAddMmTasks(Table tbl, Long writeId) throws HiveException {
    // We will move all the files in the table/partition directories into the first MM
    // directory, then commit the first write ID.
    List<Path> srcs = new ArrayList<>(), tgts = new ArrayList<>();
    if (writeId == null) {
      throw new HiveException("Internal error - write ID not set for MM conversion");
    }

    int stmtId = 0;
    String mmDir = AcidUtils.deltaSubdir(writeId, writeId, stmtId);

    Hive db = getHive();
    if (tbl.getPartitionKeys().size() > 0) {
      PartitionIterable parts = new PartitionIterable(db, tbl, null,
          HiveConf.getIntVar(conf, ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
      Iterator<Partition> partIter = parts.iterator();
      while (partIter.hasNext()) {
        Partition part = partIter.next();
        checkMmLb(part);
        Path src = part.getDataLocation(), tgt = new Path(src, mmDir);
        srcs.add(src);
        tgts.add(tgt);
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("Will move " + src + " to " + tgt);
        }
      }
    } else {
      checkMmLb(tbl);
      Path src = tbl.getDataLocation(), tgt = new Path(src, mmDir);
      srcs.add(src);
      tgts.add(tgt);
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("Will move " + src + " to " + tgt);
      }
    }
    // Don't set inputs and outputs - the locks have already been taken so it's pointless.
    MoveWork mw = new MoveWork(null, null, null, null, false);
    mw.setMultiFilesDesc(new LoadMultiFilesDesc(srcs, tgts, true, null, null));
    return Lists.<Task<?>>newArrayList(TaskFactory.get(mw));
  }

  private List<Task<?>> alterTableAddProps(AlterTableDesc alterTbl, Table tbl,
      Partition part, EnvironmentContext environmentContext) throws HiveException {
    if (StatsSetupConst.USER.equals(environmentContext.getProperties()
        .get(StatsSetupConst.STATS_GENERATED))) {
      environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
    }
    List<Task<?>> result = null;
    if (part != null) {
      part.getTPartition().getParameters().putAll(alterTbl.getProps());
    } else {
      boolean isFromMmTable = AcidUtils.isInsertOnlyTable(tbl.getParameters());
      Boolean isToMmTable = AcidUtils.isToInsertOnlyTable(tbl, alterTbl.getProps());
      if (isToMmTable != null) {
        if (!isFromMmTable && isToMmTable) {
          if (!HiveConf.getBoolVar(conf, ConfVars.HIVE_MM_ALLOW_ORIGINALS)) {
            result = generateAddMmTasks(tbl, alterTbl.getWriteId());
          } else {
            if (tbl.getPartitionKeys().size() > 0) {
              Hive db = getHive();
              PartitionIterable parts = new PartitionIterable(db, tbl, null,
                  HiveConf.getIntVar(conf, ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
              Iterator<Partition> partIter = parts.iterator();
              while (partIter.hasNext()) {
                Partition part0 = partIter.next();
                checkMmLb(part0);
              }
            } else {
              checkMmLb(tbl);
            }
          }
        } else if (isFromMmTable && !isToMmTable) {
          throw new HiveException("Cannot convert an ACID table to non-ACID");
        }
      }

      // Converting to/from external table
      String externalProp = alterTbl.getProps().get("EXTERNAL");
      if (externalProp != null) {
        if (Boolean.parseBoolean(externalProp) && tbl.getTableType() == TableType.MANAGED_TABLE) {
          tbl.setTableType(TableType.EXTERNAL_TABLE);
        } else if (!Boolean.parseBoolean(externalProp) && tbl.getTableType() == TableType.EXTERNAL_TABLE) {
          tbl.setTableType(TableType.MANAGED_TABLE);
        }
      }

      tbl.getTTable().getParameters().putAll(alterTbl.getProps());
    }
    return result;
  }

  private int dropConstraint(Hive db, AlterTableDesc alterTbl)
          throws SemanticException, HiveException {
    try {
     db.dropConstraint(Utilities.getDatabaseName(alterTbl.getOldName()),
       Utilities.getTableName(alterTbl.getOldName()),
         alterTbl.getConstraintName());
     } catch (NoSuchObjectException e) {
       throw new HiveException(e);
     }
    return 0;
  }

  private int addConstraints(Hive db, AlterTableDesc alterTbl)
           throws SemanticException, HiveException {
    try {
      // This is either an alter table add foreign key or add primary key command.
      if (alterTbl.getPrimaryKeyCols() != null && !alterTbl.getPrimaryKeyCols().isEmpty()) {
        db.addPrimaryKey(alterTbl.getPrimaryKeyCols());
      }
      if (alterTbl.getForeignKeyCols() != null && !alterTbl.getForeignKeyCols().isEmpty()) {
        try {
          db.addForeignKey(alterTbl.getForeignKeyCols());
        } catch (HiveException e) {
          if (e.getCause() instanceof InvalidObjectException
              && alterTbl.getReplicationSpec()!= null && alterTbl.getReplicationSpec().isInReplicationScope()) {
            // During repl load, NoSuchObjectException in foreign key shall
            // ignore as the foreign table may not be part of the replication
            LOG.debug("InvalidObjectException: ", e);
          } else {
            throw e;
          }
        }
      }
      if (alterTbl.getUniqueConstraintCols() != null
              && !alterTbl.getUniqueConstraintCols().isEmpty()) {
        db.addUniqueConstraint(alterTbl.getUniqueConstraintCols());
      }
      if (alterTbl.getNotNullConstraintCols() != null
              && !alterTbl.getNotNullConstraintCols().isEmpty()) {
        db.addNotNullConstraint(alterTbl.getNotNullConstraintCols());
      }
      if (alterTbl.getDefaultConstraintCols() != null
          && !alterTbl.getDefaultConstraintCols().isEmpty()) {
        db.addDefaultConstraint(alterTbl.getDefaultConstraintCols());
      }
      if (alterTbl.getCheckConstraintCols() != null
          && !alterTbl.getCheckConstraintCols().isEmpty()) {
        db.addCheckConstraint(alterTbl.getCheckConstraintCols());
      }
    } catch (NoSuchObjectException e) {
      throw new HiveException(e);
    }
    return 0;
  }

  private int updateColumns(Table tbl, Partition part)
          throws HiveException {
    String serializationLib = tbl.getSd().getSerdeInfo().getSerializationLib();
    if (MetastoreConf.getStringCollection(conf,
            MetastoreConf.ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA).contains(serializationLib)) {
      throw new HiveException(tbl.getTableName() + " has serde " + serializationLib + " for which schema " +
              "is already handled by HMS.");
    }
    Deserializer deserializer = tbl.getDeserializer(true);
    try {
      LOG.info("Updating metastore columns for table: {}", tbl.getTableName());
      final List<FieldSchema> fields = HiveMetaStoreUtils.getFieldsFromDeserializer(
              tbl.getTableName(), deserializer);
      StorageDescriptor sd = retrieveStorageDescriptor(tbl, part);
      sd.setCols(fields);
    } catch (org.apache.hadoop.hive.serde2.SerDeException | MetaException e) {
      LOG.error("alter table update columns: {}", e);
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    return 0;
  }

   /**
   * Drop a given partitions.
   *
   * @param db
   *          The database in question.
   * @param dropPartition
   *          This is the partition we're dropping.
   * @throws HiveException
   *           Throws this exception if an unexpected error occurs.
   */
  private void dropPartitions(Hive db, DropPartitionDesc dropPartition) throws HiveException {
    // We need to fetch the table before it is dropped so that it can be passed to
    // post-execution hook
    Table tbl = null;
    try {
      tbl = db.getTable(dropPartition.getTableName());
    } catch (InvalidTableException e) {
      // drop table is idempotent
    }

    ReplicationSpec replicationSpec = dropPartition.getReplicationSpec();
    if (replicationSpec.isInReplicationScope()){
      /**
       * ALTER TABLE DROP PARTITION ... FOR REPLICATION(x) behaves as a DROP PARTITION IF OLDER THAN x
       *
       * So, we check each partition that matches our DropTableDesc.getPartSpecs(), and drop it only
       * if it's older than the event that spawned this replicated request to drop partition
       */
      // TODO: Current implementation of replication will result in DROP_PARTITION under replication
      // scope being called per-partition instead of multiple partitions. However, to be robust, we
      // must still handle the case of multiple partitions in case this assumption changes in the
      // future. However, if this assumption changes, we will not be very performant if we fetch
      // each partition one-by-one, and then decide on inspection whether or not this is a candidate
      // for dropping. Thus, we need a way to push this filter (replicationSpec.allowEventReplacementInto)
      // to the  metastore to allow it to do drop a partition or not, depending on a Predicate on the
      // parameter key values.

      if (tbl == null) {
        // If table is missing, then partitions are also would've been dropped. Just no-op.
        return;
      }

      for (DropPartitionDesc.PartSpec partSpec : dropPartition.getPartSpecs()){
        List<Partition> partitions = new ArrayList<>();
        try {
          db.getPartitionsByExpr(tbl, partSpec.getPartSpec(), conf, partitions);
          for (Partition p : Iterables.filter(partitions,
              replicationSpec.allowEventReplacementInto())){
            db.dropPartition(tbl.getDbName(),tbl.getTableName(),p.getValues(),true);
          }
        } catch (NoSuchObjectException e){
          // ignore NSOE because that means there's nothing to drop.
        } catch (Exception e) {
          throw new HiveException(e.getMessage(), e);
        }
      }
      return;
    }

    // ifExists is currently verified in DDLSemanticAnalyzer
    List<Partition> droppedParts
        = db.dropPartitions(dropPartition.getTableName(),
                            dropPartition.getPartSpecs(),
                            PartitionDropOptions.instance()
                                                .deleteData(true)
                                                .ifExists(true)
                                                .purgeData(dropPartition.getIfPurge()));
    for (Partition partition : droppedParts) {
      console.printInfo("Dropped the partition " + partition.getName());
      // We have already locked the table, don't lock the partitions.
      addIfAbsentByName(new WriteEntity(partition, WriteEntity.WriteType.DDL_NO_LOCK));
    }
  }

  /**
   * Update last_modified_by and last_modified_time parameters in parameter map.
   *
   * @param params
   *          Parameters.
   * @param conf
   *          HiveConf of session
   */
  private boolean updateModifiedParameters(Map<String, String> params, HiveConf conf) throws HiveException {
    String user = null;
    user = SessionState.getUserFromAuthenticator();
    params.put("last_modified_by", user);
    params.put("last_modified_time", Long.toString(System.currentTimeMillis() / 1000));
    return true;
  }

  /**
   * Check if the given serde is valid.
   */
  public static void validateSerDe(String serdeName, HiveConf conf) throws HiveException {
    try {

      Deserializer d = ReflectionUtil.newInstance(conf.getClassByName(serdeName).
          asSubclass(Deserializer.class), conf);
      if (d != null) {
        LOG.debug("Found class for {}", serdeName);
      }
    } catch (Exception e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  private int exchangeTablePartition(Hive db,
      AlterTableExchangePartition exchangePartition) throws HiveException {
    Map<String, String> partitionSpecs = exchangePartition.getPartitionSpecs();
    Table destTable = exchangePartition.getDestinationTable();
    Table sourceTable = exchangePartition.getSourceTable();
    List<Partition> partitions =
        db.exchangeTablePartitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(),destTable.getDbName(),
        destTable.getTableName());

    for(Partition partition : partitions) {
      // Reuse the partition specs from dest partition since they should be the same
      work.getInputs().add(new ReadEntity(new Partition(sourceTable, partition.getSpec(), null)));

      addIfAbsentByName(new WriteEntity(new Partition(sourceTable, partition.getSpec(), null),
          WriteEntity.WriteType.DELETE));

      addIfAbsentByName(new WriteEntity(new Partition(destTable, partition.getSpec(), null),
          WriteEntity.WriteType.INSERT));
    }

    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return "DDL";
  }

  /**
   * Validate if the given table/partition is eligible for update
   *
   * @param db Database.
   * @param tableName Table name of format db.table
   * @param partSpec Partition spec for the partition
   * @param replicationSpec Replications specification
   *
   * @return boolean true if allow the operation
   * @throws HiveException
   */
  private boolean allowOperationInReplicationScope(Hive db, String tableName,
              Map<String, String> partSpec, ReplicationSpec replicationSpec) throws HiveException {
    if ((null == replicationSpec) || (!replicationSpec.isInReplicationScope())) {
      // Always allow the operation if it is not in replication scope.
      return true;
    }
    // If the table/partition exist and is older than the event, then just apply
    // the event else noop.
    Table existingTable = db.getTable(tableName, false);
    if ((existingTable != null)
            && replicationSpec.allowEventReplacementInto(existingTable.getParameters())) {
      // Table exists and is older than the update. Now, need to ensure if update allowed on the
      // partition.
      if (partSpec != null) {
        Partition existingPtn = db.getPartition(existingTable, partSpec, false);
        return ((existingPtn != null)
                && replicationSpec.allowEventReplacementInto(existingPtn.getParameters()));
      }

      // Replacement is allowed as the existing table is older than event
      return true;
    }

    // The table is missing either due to drop/rename which follows the operation.
    // Or the existing table is newer than our update. So, don't allow the update.
    return false;
  }

  private int remFirstIncPendFlag(Hive hive, ReplRemoveFirstIncLoadPendFlagDesc desc) throws HiveException, TException {
    String dbNameOrPattern = desc.getDatabaseName();
    String tableNameOrPattern = desc.getTableName();
    Map<String, String> parameters;
    // For database level load tableNameOrPattern will be null. Flag is set only in database for db level load.
    if (tableNameOrPattern != null && !tableNameOrPattern.isEmpty()) {
      // For table level load, dbNameOrPattern is db name and not a pattern.
      for (String tableName : Utils.matchesTbl(hive, dbNameOrPattern, tableNameOrPattern)) {
        org.apache.hadoop.hive.metastore.api.Table tbl = hive.getMSC().getTable(dbNameOrPattern, tableName);
        parameters = tbl.getParameters();
        String incPendPara = parameters != null ? parameters.get(ReplUtils.REPL_FIRST_INC_PENDING_FLAG) : null;
        if (incPendPara != null) {
          parameters.remove(ReplUtils.REPL_FIRST_INC_PENDING_FLAG);
          hive.getMSC().alter_table(dbNameOrPattern, tableName, tbl);
        }
      }
    } else {
      for (String dbName : Utils.matchesDb(hive, dbNameOrPattern)) {
        Database database = hive.getMSC().getDatabase(dbName);
        parameters = database.getParameters();
        String incPendPara = parameters != null ? parameters.get(ReplUtils.REPL_FIRST_INC_PENDING_FLAG) : null;
        if (incPendPara != null) {
          parameters.remove(ReplUtils.REPL_FIRST_INC_PENDING_FLAG);
          hive.getMSC().alterDatabase(dbName, database);
        }
      }
    }
    return 0;
  }

  /*
  uses the authorizer from SessionState will need some more work to get this to run in parallel,
  however this should not be a bottle neck so might not need to parallelize this.
   */
  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
