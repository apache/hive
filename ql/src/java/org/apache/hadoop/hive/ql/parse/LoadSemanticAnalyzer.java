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

import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.mapred.InputFormat;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoadSemanticAnalyzer.
 *
 */
public class LoadSemanticAnalyzer extends SemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(LoadSemanticAnalyzer.class);
  private boolean queryReWritten = false;

  private final String tempTblNameSuffix = "__TEMP_TABLE_FOR_LOAD_DATA__";

  // AST specific data
  private Tree fromTree, tableTree;
  private boolean isLocal = false, isOverWrite = false;

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

  private URI initializeFromURI(String fromPath, boolean isLocal) throws IOException,
      URISyntaxException {
    URI fromURI = new Path(fromPath).toUri();

    String fromScheme = fromURI.getScheme();
    String fromAuthority = fromURI.getAuthority();
    String path = fromURI.getPath();

    // generate absolute path relative to current directory or hdfs home
    // directory
    if (!path.startsWith("/")) {
      if (isLocal) {
        path = URIUtil.decode(
            new Path(System.getProperty("user.dir"), fromPath).toUri().toString());
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
      throw new SemanticException(ErrorMsg.ILLEGAL_PATH.getMsg(fromTree,
          "Source file system should be \"file\" if \"local\" is specified"));
    }

    try {
      FileSystem fileSystem = FileSystem.get(fromURI, conf);
      srcs = matchFilesOrDir(fileSystem, new Path(fromURI));
      if (srcs == null || srcs.length == 0) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(fromTree,
            "No files matching path " + fromURI));
      }

      for (FileStatus oneSrc : srcs) {
        if (oneSrc.isDir()) {
          reparseAndSuperAnalyze(table, fromURI);
          return null;
        }
      }
      validateAcidFiles(table, srcs, fileSystem);
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
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(fromTree), e);
    }

    return Lists.newArrayList(srcs);
  }

  /**
   * Safety check to make sure a file take from one acid table is not added into another acid table
   * since the ROW__IDs embedded as part a write to one table won't make sense in different
   * table/cluster.
   */
  private static void validateAcidFiles(Table table, FileStatus[] srcs, FileSystem fs)
      throws SemanticException {
    if(!AcidUtils.isFullAcidTable(table)) {
      return;
    }
    try {
      for (FileStatus oneSrc : srcs) {
        if (!AcidUtils.MetaDataFile.isRawFormatFile(oneSrc.getPath(), fs)) {
          throw new SemanticException(ErrorMsg.LOAD_DATA_ACID_FILE, oneSrc.getPath().toString());
        }
      }
    }
    catch(IOException ex) {
      throw new SemanticException(ex);
    }
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

    if (ast.getChildCount() == 4) {
      isLocal = true;
      isOverWrite = true;
    }

    if (ast.getChildCount() == 3) {
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
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(fromTree, e
          .getMessage()), e);
    }

    // initialize destination table/partition
    TableSpec ts = new TableSpec(db, conf, (ASTNode) tableTree);

    if (ts.tableHandle.isView() || ts.tableHandle.isMaterializedView()) {
      throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
    }
    if (ts.tableHandle.isNonNative()) {
      throw new SemanticException(ErrorMsg.LOAD_INTO_NON_NATIVE.getMsg());
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
    if (queryReWritten) return;

    // for managed tables, make sure the file formats match
    if (TableType.MANAGED_TABLE.equals(ts.tableHandle.getTableType())
        && conf.getBoolVar(HiveConf.ConfVars.HIVECHECKFILEFORMAT)) {
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

    Task<? extends Serializable> childTask = TaskFactory.get(
        new MoveWork(getInputs(), getOutputs(), loadTableWork, null, true,
            isLocal)
    );

    rootTasks.add(childTask);

    // The user asked for stats to be collected.
    // Some stats like number of rows require a scan of the data
    // However, some other stats, like number of files, do not require a complete scan
    // Update the stats which do not require a complete scan.
    Task<? extends Serializable> statTask = null;
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

    // Move all the partition columns at the end of table columns
    tempTableObj.setFields(table.getAllCols());
    // wipe out partition columns
    tempTableObj.setPartCols(new ArrayList<>());

    // Set data location and input format, it must be text
    tempTableObj.setDataLocation(new Path(fromURI));
    tempTableObj.setInputFormatClass(TextInputFormat.class);

    // Step 2 : create the Insert query
    StringBuilder rewrittenQueryStr = new StringBuilder();

    rewrittenQueryStr.append("insert into table ");
    rewrittenQueryStr.append(getFullTableNameForSQL((ASTNode)(tableTree.getChild(0))));
    addPartitionColsToInsert(table.getPartCols(), rewrittenQueryStr);
    rewrittenQueryStr.append(" select * from ");
    rewrittenQueryStr.append(tempTblName);

    // Step 3 : parse the query
    // Set dynamic partitioning to nonstrict so that queries do not need any partition
    // references.
    HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    // Parse the rewritten query string
    Context rewrittenCtx;
    try {
      rewrittenCtx = new Context(conf);
      // We keep track of all the contexts that are created by this query
      // so we can clear them when we finish execution
      ctx.addRewrittenStatementContext(rewrittenCtx);
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.LOAD_DATA_LAUNCH_JOB_IO_ERROR.getMsg());
    }
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
  public HashSet<WriteEntity> getAllOutputs() {
    return outputs;
  }
}
