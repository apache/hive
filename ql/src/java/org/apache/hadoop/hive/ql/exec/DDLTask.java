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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.Msck;
import org.apache.hadoop.hive.metastore.MsckInfo;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.AlterTablePartMergeFilesDesc;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;
import org.apache.hadoop.hive.ql.plan.AlterTableSimpleDesc;
import org.apache.hadoop.hive.ql.plan.CacheMetadataDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.FileMergeDesc;
import org.apache.hadoop.hive.ql.plan.InsertCommitHookDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.OrcFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.RCFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.ReplRemoveFirstIncLoadPendFlagDesc;
import org.apache.hadoop.hive.ql.plan.ShowConfDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
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

      AlterTableDesc alterTbl = work.getAlterTblDesc();
      if (alterTbl != null) {
        if (!allowOperationInReplicationScope(db, alterTbl.getOldName(), null, alterTbl.getReplicationSpec())) {
          // no alter, the table is missing either due to drop/rename which follows the alter.
          // or the existing table is newer than our update.
          LOG.debug("DDLTask: Alter Table is skipped as table {} is newer than update", alterTbl.getOldName());
          return 0;
        }
        return alterTable(db, alterTbl);
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

      ShowConfDesc showConf = work.getShowConfDesc();
      if (showConf != null) {
        return showConf(db, showConf);
      }

      AlterTablePartMergeFilesDesc mergeFilesDesc = work.getMergeFilesDesc();
      if (mergeFilesDesc != null) {
        return mergeFiles(db, mergeFilesDesc, driverContext);
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
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.ADDPROPS) {
      return alterTableAddProps(alterTbl, tbl, part, environmentContext);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.DROPPROPS) {
      return alterTableDropProps(alterTbl, tbl, part, environmentContext);
    } else if (alterTbl.getOp() == AlterTableDesc.AlterTableTypes.OWNER) {
      if (alterTbl.getOwnerPrincipal() != null) {
        tbl.setOwner(alterTbl.getOwnerPrincipal().getName());
        tbl.setOwnerType(alterTbl.getOwnerPrincipal().getType());
      }
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
