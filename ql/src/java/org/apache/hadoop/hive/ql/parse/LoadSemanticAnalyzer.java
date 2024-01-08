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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_LOAD_DATA_USE_NATIVE_API;

/**
 * LoadSemanticAnalyzer.
 *
 */
public class LoadSemanticAnalyzer extends SemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(LoadSemanticAnalyzer.class);
  private boolean queryReWritten = false;

  private final String tempTblNameSuffix = "__temp_table_for_load_data__";

  // AST specific data
  private Tree fromTree, tableTree;
  private boolean isLocal = false, isOverWrite = false;
  private String inputFormatClassName = null;
  private String serDeClassName = null;

  public LoadSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  public static FileStatus[] matchFilesOrDir(FileSystem fs, Path path)
      throws IOException {
    FileStatus[] srcs = fs.globStatus(path, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        String name = p.getName();
        return name.equals(EximUtil.METADATA_NAME) || (!name.startsWith("_") && !name.startsWith("."));
      }
    });
    if ((srcs != null) && srcs.length == 1) {
      if (srcs[0].isDirectory()) {
        srcs = fs.listStatus(srcs[0].getPath(), new PathFilter() {
          @Override
          public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
          }
        });
      }
    }
    return (srcs);
  }

  private URI initializeFromURI(String fromPath, boolean isLocal)
      throws IOException, URISyntaxException, SemanticException {
    URI fromURI = new Path(fromPath).toUri();

    String fromScheme = fromURI.getScheme();
    String fromAuthority = fromURI.getAuthority();
    String path = fromURI.getPath();

    // generate absolute path relative to current directory or hdfs home
    // directory
    if (!path.startsWith("/")) {
      if (isLocal) {
        try {
          path = new String(URLCodec.decodeUrl(
              new Path(System.getProperty("user.dir"), fromPath).toUri().toString()
                  .getBytes("US-ASCII")), "US-ASCII");
        } catch (DecoderException de) {
          throw new SemanticException("URL Decode failed", de);
        }
      } else {
        path = new Path(new Path("/user/" + System.getProperty("user.name")),
          path).toString();
      }
    }

    // set correct scheme and authority
    if (StringUtils.isEmpty(fromScheme)) {
      if (isLocal) {
        // file for local
        fromScheme = "file";
      } else {
        // use default values from fs.default.name
        URI defaultURI = FileSystem.get(conf).getUri();
        fromScheme = defaultURI.getScheme();
        fromAuthority = defaultURI.getAuthority();
      }
    }

    // if scheme is specified but not authority then use the default authority
    if ((!fromScheme.equals("file")) && StringUtils.isEmpty(fromAuthority)) {
      URI defaultURI = FileSystem.get(conf).getUri();
      fromAuthority = defaultURI.getAuthority();
    }

    LOG.debug(fromScheme + "@" + fromAuthority + "@" + path);
    return new URI(fromScheme, fromAuthority, path, null, null);
  }

  private List<FileStatus> applyConstraintsAndGetFiles(URI fromURI, Table table) throws SemanticException {

    FileStatus[] srcs = null;

    // local mode implies that scheme should be "file"
    // we can change this going forward
    if (isLocal && !fromURI.getScheme().equals("file")) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.ILLEGAL_PATH.getMsg(), fromTree,
          "Source file system should be \"file\" if \"local\" is specified"));
    }

    try {
      FileSystem fileSystem = FileSystem.get(fromURI, conf);
      srcs = matchFilesOrDir(fileSystem, new Path(fromURI));
      if (srcs == null || srcs.length == 0) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_PATH.getMsg(), fromTree,
            "No files matching path " + fromURI));
      }

      for (FileStatus oneSrc : srcs) {
        if (oneSrc.isDir()) {
          reparseAndSuperAnalyze(table, fromURI);
          return null;
        }
      }
      AcidUtils.validateAcidFiles(table, srcs, fileSystem);
      // Do another loop if table is bucketed
      List<String> bucketCols = table.getBucketCols();
      if (bucketCols != null && !bucketCols.isEmpty()) {
        // Hive assumes that user names the files as per the corresponding
        // bucket. For e.g, file names should follow the format 000000_0, 000000_1 etc.
        // Here the 1st file will belong to bucket 0 and 2nd to bucket 1 and so on.
        boolean[] bucketArray = new boolean[table.getNumBuckets()];
        // initialize the array
        Arrays.fill(bucketArray, false);
        int numBuckets = table.getNumBuckets();

        for (FileStatus oneSrc : srcs) {
          String bucketName = oneSrc.getPath().getName();

          //get the bucket id
          String bucketIdStr =
                  Utilities.getBucketFileNameFromPathSubString(bucketName);
          int bucketId = Utilities.getBucketIdFromFile(bucketIdStr);
          LOG.debug("bucket ID for file " + oneSrc.getPath() + " = " + bucketId
          + " for table " + table.getFullyQualifiedName());
          if (bucketId == -1 || bucketId >= numBuckets || bucketArray[bucketId]) {
            reparseAndSuperAnalyze(table, fromURI);
            return null;
          }
          bucketArray[bucketId] = true;
        }
      }
    } catch (IOException e) {
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.INVALID_PATH.getMsg(), fromTree), e);
    }

    return Lists.newArrayList(srcs);
  }


  @Override
  public void init(boolean clearPartsCache) {
    Table tempTable = ctx.getTempTableForLoad();
    if (tempTable != null) {
      // tempTable is only set when load is rewritten.
      super.init(clearPartsCache);
      tabNameToTabObject.put(tempTable.getTableName().toLowerCase(), tempTable);
    }
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    if (ctx.getTempTableForLoad() != null) {
      super.analyzeInternal(ast);
    } else {
      analyzeLoad(ast);
    }
  }

  private void analyzeLoad(ASTNode ast) throws SemanticException {
    fromTree = ast.getChild(0);
    tableTree = ast.getChild(1);

    boolean inputInfo = false;
    // Check the last node
    ASTNode child = (ASTNode)ast.getChild(ast.getChildCount() - 1);
    if (child.getToken().getType() == HiveParser.TOK_INPUTFORMAT) {
      if (child.getChildCount() != 2) {
        throw new SemanticException("FileFormat should contain both input format and Serde");
      }
      try {
        inputFormatClassName = stripQuotes(child.getChild(0).getText());
        serDeClassName = stripQuotes(child.getChild(1).getText());
        inputInfo = true;
      } catch (Exception e) {
        throw new SemanticException("FileFormat inputFormatClassName or serDeClassName is incorrect");
      }
    }

    if ((!inputInfo && ast.getChildCount() == 4) ||
        (inputInfo && ast.getChildCount() == 5)) {
      isLocal = true;
      isOverWrite = true;
    }

    if ((!inputInfo && ast.getChildCount() == 3) ||
        (inputInfo && ast.getChildCount() == 4)) {
      if (ast.getChild(2).getText().toLowerCase().equals("local")) {
        isLocal = true;
      } else {
        isOverWrite = true;
      }
    }

    // initialize load path
    URI fromURI;
    try {
      String fromPath = stripQuotes(fromTree.getText());
      fromURI = initializeFromURI(fromPath, isLocal);
    } catch (IOException | URISyntaxException e) {
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.INVALID_PATH.getMsg(), fromTree, e.getMessage()), e);
    }

    // initialize destination table/partition
    TableSpec ts = new TableSpec(db, conf, (ASTNode) tableTree);

    if (ts.tableHandle.isView() || ts.tableHandle.isMaterializedView()) {
      throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
    }
    if (ts.tableHandle.isNonNative()) {
      HiveStorageHandler storageHandler = ts.tableHandle.getStorageHandler();
      boolean isUseNativeApi = conf.getBoolVar(HIVE_LOAD_DATA_USE_NATIVE_API);
      boolean supportAppend = isUseNativeApi && storageHandler.supportsAppendData(ts.tableHandle.getTTable(),
          ts.getPartSpec() != null && !ts.getPartSpec().isEmpty());
      if (supportAppend) {
        LoadTableDesc loadTableWork =
            new LoadTableDesc(new Path(fromURI), ts.tableHandle, isOverWrite, true, ts.getPartSpec());
        Task<?> childTask =
            TaskFactory.get(new MoveWork(getInputs(), getOutputs(), loadTableWork, null, true, isLocal));
        rootTasks.add(childTask);
        return;
      } else {
        // launch a tez job
        StorageFormatDescriptor ss = storageHandler.getStorageFormatDescriptor(ts.tableHandle.getTTable());
        if (ss != null) {
          inputFormatClassName = ss.getInputFormat();
          serDeClassName = ss.getSerde();
          reparseAndSuperAnalyze(ts.tableHandle, fromURI);
          return;
        }
        throw new SemanticException(ErrorMsg.LOAD_INTO_NON_NATIVE.getMsg());
      }
    }

    if(ts.tableHandle.isStoredAsSubDirectories()) {
      throw new SemanticException(ErrorMsg.LOAD_INTO_STORED_AS_DIR.getMsg());
    }
    List<FieldSchema> parts = ts.tableHandle.getPartitionKeys();
    if ((parts != null && parts.size() > 0)
        && (ts.partSpec == null || ts.partSpec.size() == 0)) {
      // launch a tez job
      reparseAndSuperAnalyze(ts.tableHandle, fromURI);
      return;
    }

    List<String> bucketCols = ts.tableHandle.getBucketCols();
    if (bucketCols != null && !bucketCols.isEmpty()) {
      String error = StrictChecks.checkBucketing(conf);
      if (error != null) {
        // launch a tez job
        reparseAndSuperAnalyze(ts.tableHandle, fromURI);
        return;
      }
    }

    // make sure the arguments make sense
    List<FileStatus> files = applyConstraintsAndGetFiles(fromURI, ts.tableHandle);
    if (queryReWritten) {
      return;
    }

    // for managed tables, make sure the file formats match
    if (TableType.MANAGED_TABLE.equals(ts.tableHandle.getTableType())
        && conf.getBoolVar(HiveConf.ConfVars.HIVE_CHECK_FILEFORMAT)) {
      ensureFileFormatsMatch(ts, files, fromURI);
    }
    inputs.add(toReadEntity(new Path(fromURI)));

    // create final load/move work

    boolean preservePartitionSpecs = false;

    Map<String, String> partSpec = ts.getPartSpec();
    if (partSpec == null) {
      partSpec = new LinkedHashMap<String, String>();
      outputs.add(new WriteEntity(ts.tableHandle,
          (isOverWrite ? WriteEntity.WriteType.INSERT_OVERWRITE :
              WriteEntity.WriteType.INSERT)));
    } else {
      try{
        Partition part = Hive.get().getPartition(ts.tableHandle, partSpec, false);
        if (part != null) {
          if (isOverWrite){
            outputs.add(new WriteEntity(part, WriteEntity.WriteType.INSERT_OVERWRITE));
          } else {
            outputs.add(new WriteEntity(part, WriteEntity.WriteType.INSERT));
            // If partition already exists and we aren't overwriting it, then respect
            // its current location info rather than picking it from the parent TableDesc
            preservePartitionSpecs = true;
          }
        } else {
          outputs.add(new WriteEntity(ts.tableHandle,
          (isOverWrite ? WriteEntity.WriteType.INSERT_OVERWRITE :
              WriteEntity.WriteType.INSERT)));
        }
      } catch(HiveException e) {
        throw new SemanticException(e);
      }
    }

    Long writeId = null;
    int stmtId = -1;
    boolean isTxnTable = AcidUtils.isTransactionalTable(ts.tableHandle);
    if (isTxnTable) {
      try {
        writeId = getTxnMgr().getTableWriteId(ts.tableHandle.getDbName(),
                ts.tableHandle.getTableName());
      } catch (LockException ex) {
        throw new SemanticException("Failed to allocate the write id", ex);
      }
      stmtId = getTxnMgr().getStmtIdAndIncrement();
    }

    // Note: this sets LoadFileType incorrectly for ACID; is that relevant for load?
    //       See setLoadFileType and setIsAcidIow calls elsewhere for an example.
    LoadTableDesc loadTableWork = new LoadTableDesc(new Path(fromURI),
      Utilities.getTableDesc(ts.tableHandle), partSpec, isOverWrite
        ? LoadFileType.REPLACE_ALL : LoadFileType.KEEP_EXISTING, writeId);
    loadTableWork.setStmtId(stmtId);
    loadTableWork.setInsertOverwrite(isOverWrite);
    if (preservePartitionSpecs) {
      // Note : preservePartitionSpecs=true implies inheritTableSpecs=false but
      // but preservePartitionSpecs=false(default) here is not sufficient enough
      // info to set inheritTableSpecs=true
      loadTableWork.setInheritTableSpecs(false);
    }

    Task<?> childTask = TaskFactory.get(
        new MoveWork(getInputs(), getOutputs(), loadTableWork, null, true, isLocal)
    );

    rootTasks.add(childTask);

    // The user asked for stats to be collected.
    // Some stats like number of rows require a scan of the data
    // However, some other stats, like number of files, do not require a complete scan
    // Update the stats which do not require a complete scan.
    Task<?> statTask = null;
    if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      BasicStatsWork basicStatsWork = new BasicStatsWork(loadTableWork);
      basicStatsWork.setNoStatsAggregator(true);
      basicStatsWork.setClearAggregatorStats(true);
      StatsWork columnStatsWork = new StatsWork(ts.tableHandle, basicStatsWork, conf);
      statTask = TaskFactory.get(columnStatsWork);
    }

    if (statTask != null) {
      childTask.addDependentTask(statTask);
    }
  }

  private void ensureFileFormatsMatch(TableSpec ts, List<FileStatus> fileStatuses,
      final URI fromURI)
      throws SemanticException {
    final Class<? extends InputFormat> destInputFormat;
    try {
      if (ts.getPartSpec() == null || ts.getPartSpec().isEmpty()) {
        destInputFormat = ts.tableHandle.getInputFormatClass();
      } else {
        destInputFormat = ts.partHandle.getInputFormatClass();
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    try {
      FileSystem fs = FileSystem.get(fromURI, conf);
      boolean validFormat = HiveFileFormatUtils.checkInputFormat(fs, conf, destInputFormat,
          fileStatuses);
      if (!validFormat) {
        throw new SemanticException(ErrorMsg.INVALID_FILE_FORMAT_IN_LOAD.getMsg());
      }
    } catch (Exception e) {
      throw new SemanticException("Unable to load data to destination table." +
          " Error: " + e.getMessage());
    }
  }

  // Rewrite the load to launch an insert job.
  private void reparseAndSuperAnalyze(Table table, URI fromURI) throws SemanticException {
    LOG.info("Load data triggered a Tez job instead of usual file operation");
    // Step 1 : Create a temp table object
    // Create a Table object
    Table tempTableObj = new Table(new org.apache.hadoop.hive.metastore.api.Table(table.getTTable()));
    // Construct a temp table name
    String tempTblName = table.getTableName() + tempTblNameSuffix;
    tempTableObj.setTableName(tempTblName);

    // Reset table params
    tempTableObj.setParameters(new HashMap<>());

    // Set data location and input format, it must be text
    tempTableObj.setDataLocation(new Path(fromURI));
    if (inputFormatClassName != null && serDeClassName != null) {
      try {
        tempTableObj.setInputFormatClass(inputFormatClassName);
        tempTableObj.setSerializationLib(serDeClassName);
      } catch (HiveException e) {
        throw new SemanticException("Load Data: Failed to set inputFormat or SerDe");
      }
    }

    // Make the columns list for the temp table (input data file).
    // Move all the partition columns at the end of table columns.
    ArrayList<FieldSchema> colList = new ArrayList<FieldSchema>();
    colList.addAll(table.getCols());

    // inpPartSpec is a mapping from partition column name to its value.
    Map<String, String> inpPartSpec = null;

    // Partition spec was already validated by caller when create TableSpec object.
    // So, need not validate inpPartSpec here.
    List<FieldSchema> parts = table.getPartCols();
    if (tableTree.getChildCount() >= 2) {
      ASTNode partSpecNode = (ASTNode) tableTree.getChild(1);
      inpPartSpec = new HashMap<>(partSpecNode.getChildCount());

      for (int i = 0; i < partSpecNode.getChildCount(); ++i) {
        ASTNode partSpecValNode = (ASTNode) partSpecNode.getChild(i);
        String partVal = null;
        String partColName = unescapeIdentifier(partSpecValNode.getChild(0).getText().toLowerCase());

        if (partSpecValNode.getChildCount() >= 2) { // in the form of T partition (ds="2010-03-03")
          // Not stripping quotes here as we need to use it as it is while framing PARTITION clause
          // in INSERT query.
          partVal = partSpecValNode.getChild(1).getText();
        }
        inpPartSpec.put(partColName, partVal);
      }

      // Add only dynamic partition columns to the temp table (input data file).
      // For static partitions, values would be obtained from partition(key=value...) clause.
      for (FieldSchema fs : parts) {
        String partKey = fs.getName();

        // If a partition value is not there, then it is dynamic partition key.
        if (inpPartSpec.get(partKey) == null) {
          colList.add(fs);
        }
      }
    } else {
      // No static partitions specified and hence all are dynamic partition keys and need to be part
      // of temp table (input data file).
      colList.addAll(parts);
    }

    // Set columns list for temp table.
    tempTableObj.setFields(colList);

    // Wipe out partition columns
    tempTableObj.setPartCols(new ArrayList<>());

    // Step 2 : create the Insert query
    StringBuilder rewrittenQueryStr = new StringBuilder();

    if (isOverWrite) {
      rewrittenQueryStr.append("insert overwrite table ");
    } else {
      rewrittenQueryStr.append("insert into table ");
    }

    rewrittenQueryStr.append(getFullTableNameForSQL((ASTNode)(tableTree.getChild(0))));
    addPartitionColsToInsert(table.getPartCols(), inpPartSpec, rewrittenQueryStr);
    rewrittenQueryStr.append(" select * from ");
    rewrittenQueryStr.append(tempTblName);

    // Step 3 : parse the query
    // Set dynamic partitioning to nonstrict so that queries do not need any partition
    // references.
    HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE, "nonstrict");
    // Parse the rewritten query string
    Context rewrittenCtx;
    rewrittenCtx = new Context(conf);
    // We keep track of all the contexts that are created by this query
    // so we can clear them when we finish execution
    ctx.addSubContext(rewrittenCtx);
    rewrittenCtx.setExplainConfig(ctx.getExplainConfig());
    rewrittenCtx.setExplainPlan(ctx.isExplainPlan());
    rewrittenCtx.setCmd(rewrittenQueryStr.toString());
    rewrittenCtx.setTempTableForLoad(tempTableObj);

    ASTNode rewrittenTree;
    try {
      LOG.info("Going to reparse <" + ctx.getCmd() + "> as \n<" + rewrittenQueryStr.toString() + ">");
      rewrittenTree = ParseUtils.parse(rewrittenQueryStr.toString(), rewrittenCtx);
    } catch (ParseException e) {
      throw new SemanticException(ErrorMsg.LOAD_DATA_LAUNCH_JOB_PARSE_ERROR.getMsg(), e);
    }

    // Step 4 : Reanalyze
    super.analyze(rewrittenTree, rewrittenCtx);

    queryReWritten = true;
  }

  @Override
  public Set<WriteEntity> getAllOutputs() {
    return outputs;
  }
}
