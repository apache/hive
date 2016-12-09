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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.merge.MergeFileTask;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.BucketCol;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.SortCol;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MoveTask implementation.
 **/
public class MoveTask extends Task<MoveWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static transient final Logger LOG = LoggerFactory.getLogger(MoveTask.class);

  public MoveTask() {
    super();
  }

  private void moveFile(Path sourcePath, Path targetPath, boolean isDfsDir)
      throws HiveException {
    try {
      String mesg = "Moving data to " + (isDfsDir ? "" : "local ") + "directory "
          + targetPath.toString();
      String mesg_detail = " from " + sourcePath.toString();
      console.printInfo(mesg, mesg_detail);

      FileSystem fs = sourcePath.getFileSystem(conf);
      if (isDfsDir) {
        moveFileInDfs (sourcePath, targetPath, fs);
      } else {
        // This is a local file
        FileSystem dstFs = FileSystem.getLocal(conf);
        moveFileFromDfsToLocal(sourcePath, targetPath, fs, dstFs);
      }
    } catch (Exception e) {
      throw new HiveException("Unable to move source " + sourcePath + " to destination "
          + targetPath, e);
    }
  }

  private void moveFileInDfs (Path sourcePath, Path targetPath, FileSystem fs)
      throws HiveException, IOException {
    // if source exists, rename. Otherwise, create a empty directory
    if (fs.exists(sourcePath)) {
      Path deletePath = null;
      // If it multiple level of folder are there fs.rename is failing so first
      // create the targetpath.getParent() if it not exist
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_INSERT_INTO_MULTILEVEL_DIRS)) {
        deletePath = createTargetPath(targetPath, fs);
      }
      Hive.clearDestForSubDirSrc(conf, targetPath, sourcePath, false);
      if (!Hive.moveFile(conf, sourcePath, targetPath, true, false)) {
        try {
          if (deletePath != null) {
            fs.delete(deletePath, true);
          }
        } catch (IOException e) {
          LOG.info("Unable to delete the path created for facilitating rename"
              + deletePath);
        }
        throw new HiveException("Unable to rename: " + sourcePath
            + " to: " + targetPath);
      }
    } else if (!fs.mkdirs(targetPath)) {
      throw new HiveException("Unable to make directory: " + targetPath);
    }
  }

  private void moveFileFromDfsToLocal(Path sourcePath, Path targetPath, FileSystem fs,
      FileSystem dstFs) throws HiveException, IOException {
      // RawLocalFileSystem seems not able to get the right permissions for a local file, it
      // always returns hdfs default permission (00666). So we can not overwrite a directory
      // by deleting and recreating the directory and restoring its permissions. We should
      // delete all its files and subdirectories instead.
    if (dstFs.exists(targetPath)) {
      if (dstFs.isDirectory(targetPath)) {
        FileStatus[] destFiles = dstFs.listStatus(targetPath);
        for (FileStatus destFile : destFiles) {
          if (!dstFs.delete(destFile.getPath(), true)) {
            throw new IOException("Unable to clean the destination directory: " + targetPath);
          }
        }
      } else {
        throw new HiveException("Target " + targetPath + " is not a local directory.");
      }
    } else {
      if (!FileUtils.mkdir(dstFs, targetPath, false, conf)) {
        throw new HiveException("Failed to create local target directory " + targetPath);
      }
    }

    if (fs.exists(sourcePath)) {
      FileStatus[] srcs = fs.listStatus(sourcePath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      for (FileStatus status : srcs) {
        fs.copyToLocalFile(status.getPath(), targetPath);
      }
    }
  }

  private Path createTargetPath(Path targetPath, FileSystem fs) throws IOException {
    Path deletePath = null;
    Path mkDirPath = targetPath.getParent();
    if (mkDirPath != null && !fs.exists(mkDirPath)) {
      Path actualPath = mkDirPath;
      // targetPath path is /x/y/z/1/2/3 here /x/y/z is present in the file system
      // create the structure till /x/y/z/1/2 to work rename for multilevel directory
      // and if rename fails delete the path /x/y/z/1
      // If targetPath have multilevel directories like /x/y/z/1/2/3 , /x/y/z/1/2/4
      // the renaming of the directories are not atomic the execution will happen one
      // by one
      while (actualPath != null && !fs.exists(actualPath)) {
        deletePath = actualPath;
        actualPath = actualPath.getParent();
      }
      fs.mkdirs(mkDirPath);
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS)) {
        HdfsUtils.setFullFileStatus(conf, new HdfsUtils.HadoopFileStatus(conf, fs, actualPath), fs, mkDirPath, true);
      }
    }
    return deletePath;
  }

  // Release all the locks acquired for this object
  // This becomes important for multi-table inserts when one branch may take much more
  // time than the others. It is better to release the lock for this particular insert.
  // The other option is to wait for all the branches to finish, or set
  // hive.multi.insert.move.tasks.share.dependencies to true, which will mean that the
  // first multi-insert results will be available when all of the branches of multi-table
  // inserts are done.
  private void releaseLocks(LoadTableDesc ltd) throws HiveException {
    // nothing needs to be done
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY)) {
      return;
    }

    Context ctx = driverContext.getCtx();
    if(ctx.getHiveTxnManager().supportsAcid()) {
      //Acid LM doesn't maintain getOutputLockObjects(); this 'if' just makes it more explicit
      return;
    }
    HiveLockManager lockMgr = ctx.getHiveTxnManager().getLockManager();
    WriteEntity output = ctx.getLoadTableOutputMap().get(ltd);
    List<HiveLockObj> lockObjects = ctx.getOutputLockObjects().get(output);
    if (lockObjects == null) {
      return;
    }

    for (HiveLockObj lockObj : lockObjects) {
      List<HiveLock> locks = lockMgr.getLocks(lockObj.getObj(), false, true);
      for (HiveLock lock : locks) {
        if (lock.getHiveLockMode() == lockObj.getMode()) {
          if (ctx.getHiveLocks().remove(lock)) {
            LOG.info("about to release lock for output: " + output.toString() +
                " lock: " + lock.getHiveLockObject().getName());
            try {
              lockMgr.unlock(lock);
            } catch (LockException le) {
              // should be OK since the lock is ephemeral and will eventually be deleted
              // when the query finishes and zookeeper session is closed.
              LOG.warn("Could not release lock " + lock.getHiveLockObject().getName());
            }
          }
        }
      }
    }
  }

  // we check if there is only one immediate child task and it is stats task
  public boolean hasFollowingStatsTask() {
    if (this.getNumChild() == 1) {
      return this.getChildTasks().get(0) instanceof StatsTask;
    }
    return false;
  }

  @Override
  public int execute(DriverContext driverContext) {

    try {
      if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
        return 0;
      }
      Hive db = getHive();

      // Do any hive related operations like moving tables and files
      // to appropriate locations
      LoadFileDesc lfd = work.getLoadFileWork();
      if (lfd != null) {
        Path targetPath = lfd.getTargetDir();
        Path sourcePath = lfd.getSourcePath();
        moveFile(sourcePath, targetPath, lfd.getIsDfsDir());
      }

      // Multi-file load is for dynamic partitions when some partitions do not
      // need to merge and they can simply be moved to the target directory.
      LoadMultiFilesDesc lmfd = work.getLoadMultiFilesWork();
      if (lmfd != null) {
        boolean isDfsDir = lmfd.getIsDfsDir();
        int i = 0;
        while (i <lmfd.getSourceDirs().size()) {
          Path srcPath = lmfd.getSourceDirs().get(i);
          Path destPath = lmfd.getTargetDirs().get(i);
          FileSystem fs = destPath.getFileSystem(conf);
          if (!fs.exists(destPath.getParent())) {
            fs.mkdirs(destPath.getParent());
          }
          moveFile(srcPath, destPath, isDfsDir);
          i++;
        }
      }

      // Next we do this for tables and partitions
      LoadTableDesc tbd = work.getLoadTableWork();
      if (tbd != null) {
        StringBuilder mesg = new StringBuilder("Loading data to table ")
            .append( tbd.getTable().getTableName());
        if (tbd.getPartitionSpec().size() > 0) {
          mesg.append(" partition (");
          Map<String, String> partSpec = tbd.getPartitionSpec();
          for (String key: partSpec.keySet()) {
            mesg.append(key).append('=').append(partSpec.get(key)).append(", ");
          }
          mesg.setLength(mesg.length()-2);
          mesg.append(')');
        }
        String mesg_detail = " from " + tbd.getSourcePath();
        console.printInfo(mesg.toString(), mesg_detail);
        Table table = db.getTable(tbd.getTable().getTableName());

        if (work.getCheckFileFormat()) {
          // Get all files from the src directory
          FileStatus[] dirs;
          ArrayList<FileStatus> files;
          FileSystem srcFs; // source filesystem
          try {
            srcFs = tbd.getSourcePath().getFileSystem(conf);
            dirs = srcFs.globStatus(tbd.getSourcePath());
            files = new ArrayList<FileStatus>();
            for (int i = 0; (dirs != null && i < dirs.length); i++) {
              files.addAll(Arrays.asList(srcFs.listStatus(dirs[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER)));
              // We only check one file, so exit the loop when we have at least
              // one.
              if (files.size() > 0) {
                break;
              }
            }
          } catch (IOException e) {
            throw new HiveException(
                "addFiles: filesystem error in check phase", e);
          }

          // handle file format check for table level
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVECHECKFILEFORMAT)) {
            boolean flag = true;
            // work.checkFileFormat is set to true only for Load Task, so assumption here is
            // dynamic partition context is null
            if (tbd.getDPCtx() == null) {
              if (tbd.getPartitionSpec() == null || tbd.getPartitionSpec().isEmpty()) {
                // Check if the file format of the file matches that of the table.
                flag = HiveFileFormatUtils.checkInputFormat(
                    srcFs, conf, tbd.getTable().getInputFileFormatClass(), files);
              } else {
                // Check if the file format of the file matches that of the partition
                Partition oldPart = db.getPartition(table, tbd.getPartitionSpec(), false);
                if (oldPart == null) {
                  // this means we have just created a table and are specifying partition in the
                  // load statement (without pre-creating the partition), in which case lets use
                  // table input format class. inheritTableSpecs defaults to true so when a new
                  // partition is created later it will automatically inherit input format
                  // from table object
                  flag = HiveFileFormatUtils.checkInputFormat(
                      srcFs, conf, tbd.getTable().getInputFileFormatClass(), files);
                } else {
                  flag = HiveFileFormatUtils.checkInputFormat(
                      srcFs, conf, oldPart.getInputFormatClass(), files);
                }
              }
              if (!flag) {
                throw new HiveException(
                    "Wrong file format. Please check the file's format.");
              }
            } else {
              LOG.warn("Skipping file format check as dpCtx is not null");
            }
          }
        }

        // Create a data container
        DataContainer dc = null;
        if (tbd.getPartitionSpec().size() == 0) {
          dc = new DataContainer(table.getTTable());
          db.loadTable(tbd.getSourcePath(), tbd.getTable().getTableName(), tbd.getReplace(),
              work.isSrcLocal(), isSkewedStoredAsDirs(tbd),
              work.getLoadTableWork().getWriteType() != AcidUtils.Operation.NOT_ACID,
              hasFollowingStatsTask());
          if (work.getOutputs() != null) {
            DDLTask.addIfAbsentByName(new WriteEntity(table,
              getWriteType(tbd, work.getLoadTableWork().getWriteType())), work.getOutputs());
          }
        } else {
          LOG.info("Partition is: " + tbd.getPartitionSpec().toString());

          // Check if the bucketing and/or sorting columns were inferred
          List<BucketCol> bucketCols = null;
          List<SortCol> sortCols = null;
          int numBuckets = -1;
          Task task = this;
          String path = tbd.getSourcePath().toUri().toString();
          // Find the first ancestor of this MoveTask which is some form of map reduce task
          // (Either standard, local, or a merge)
          while (task.getParentTasks() != null && task.getParentTasks().size() == 1) {
            task = (Task)task.getParentTasks().get(0);
            // If it was a merge task or a local map reduce task, nothing can be inferred
            if (task instanceof MergeFileTask || task instanceof MapredLocalTask) {
              break;
            }

            // If it's a standard map reduce task, check what, if anything, it inferred about
            // the directory this move task is moving
            if (task instanceof MapRedTask) {
              MapredWork work = (MapredWork)task.getWork();
              MapWork mapWork = work.getMapWork();
              bucketCols = mapWork.getBucketedColsByDirectory().get(path);
              sortCols = mapWork.getSortedColsByDirectory().get(path);
              if (work.getReduceWork() != null) {
                numBuckets = work.getReduceWork().getNumReduceTasks();
              }

              if (bucketCols != null || sortCols != null) {
                // This must be a final map reduce task (the task containing the file sink
                // operator that writes the final output)
                assert work.isFinalMapRed();
              }
              break;
            }

            // If it's a move task, get the path the files were moved from, this is what any
            // preceding map reduce task inferred information about, and moving does not invalidate
            // those assumptions
            // This can happen when a conditional merge is added before the final MoveTask, but the
            // condition for merging is not met, see GenMRFileSink1.
            if (task instanceof MoveTask) {
              if (((MoveTask)task).getWork().getLoadFileWork() != null) {
                path = ((MoveTask)task).getWork().getLoadFileWork().getSourcePath().toUri().toString();
              }
            }
          }
          // deal with dynamic partitions
          DynamicPartitionCtx dpCtx = tbd.getDPCtx();
          if (dpCtx != null && dpCtx.getNumDPCols() > 0) { // dynamic partitions

            List<LinkedHashMap<String, String>> dps = Utilities.getFullDPSpecs(conf, dpCtx);

            console.printInfo(System.getProperty("line.separator"));
            long startTime = System.currentTimeMillis();
            // load the list of DP partitions and return the list of partition specs
            // TODO: In a follow-up to HIVE-1361, we should refactor loadDynamicPartitions
            // to use Utilities.getFullDPSpecs() to get the list of full partSpecs.
            // After that check the number of DPs created to not exceed the limit and
            // iterate over it and call loadPartition() here.
            // The reason we don't do inside HIVE-1361 is the latter is large and we
            // want to isolate any potential issue it may introduce.
            Map<Map<String, String>, Partition> dp =
              db.loadDynamicPartitions(
                tbd.getSourcePath(),
                tbd.getTable().getTableName(),
                tbd.getPartitionSpec(),
                tbd.getReplace(),
                dpCtx.getNumDPCols(),
                isSkewedStoredAsDirs(tbd),
                work.getLoadTableWork().getWriteType() != AcidUtils.Operation.NOT_ACID,
                SessionState.get().getTxnMgr().getCurrentTxnId(), hasFollowingStatsTask(),
                work.getLoadTableWork().getWriteType());

            // publish DP columns to its subscribers
            if (dps != null && dps.size() > 0) {
              pushFeed(FeedType.DYNAMIC_PARTITIONS, dp.values());
            }

            String loadTime = "\t Time taken to load dynamic partitions: "  +
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds";
            console.printInfo(loadTime);
            LOG.info(loadTime);

            if (dp.size() == 0 && conf.getBoolVar(HiveConf.ConfVars.HIVE_ERROR_ON_EMPTY_PARTITION)) {
              throw new HiveException("This query creates no partitions." +
                  " To turn off this error, set hive.error.on.empty.partition=false.");
            }

            startTime = System.currentTimeMillis();
            // for each partition spec, get the partition
            // and put it to WriteEntity for post-exec hook
            for(Map.Entry<Map<String, String>, Partition> entry : dp.entrySet()) {
              Partition partn = entry.getValue();

              if (bucketCols != null || sortCols != null) {
                updatePartitionBucketSortColumns(
                    db, table, partn, bucketCols, numBuckets, sortCols);
              }

              WriteEntity enty = new WriteEntity(partn,
                getWriteType(tbd, work.getLoadTableWork().getWriteType()));
              if (work.getOutputs() != null) {
                DDLTask.addIfAbsentByName(enty, work.getOutputs());
              }
              // Need to update the queryPlan's output as well so that post-exec hook get executed.
              // This is only needed for dynamic partitioning since for SP the the WriteEntity is
              // constructed at compile time and the queryPlan already contains that.
              // For DP, WriteEntity creation is deferred at this stage so we need to update
              // queryPlan here.
              if (queryPlan.getOutputs() == null) {
                queryPlan.setOutputs(new LinkedHashSet<WriteEntity>());
              }
              queryPlan.getOutputs().add(enty);

              // update columnar lineage for each partition
              dc = new DataContainer(table.getTTable(), partn.getTPartition());

              // Don't set lineage on delete as we don't have all the columns
              if (SessionState.get() != null &&
                  work.getLoadTableWork().getWriteType() != AcidUtils.Operation.DELETE &&
                  work.getLoadTableWork().getWriteType() != AcidUtils.Operation.UPDATE) {
                SessionState.get().getLineageState().setLineage(tbd.getSourcePath(), dc,
                    table.getCols());
              }
              LOG.info("\tLoading partition " + entry.getKey());
            }
            console.printInfo("\t Time taken for adding to write entity : " +
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
            dc = null; // reset data container to prevent it being added again.
          } else { // static partitions
            List<String> partVals = MetaStoreUtils.getPvals(table.getPartCols(),
                tbd.getPartitionSpec());
            db.validatePartitionNameCharacters(partVals);
            db.loadPartition(tbd.getSourcePath(), tbd.getTable().getTableName(),
                tbd.getPartitionSpec(), tbd.getReplace(),
                tbd.getInheritTableSpecs(), isSkewedStoredAsDirs(tbd), work.isSrcLocal(),
                work.getLoadTableWork().getWriteType() != AcidUtils.Operation.NOT_ACID, hasFollowingStatsTask());
            Partition partn = db.getPartition(table, tbd.getPartitionSpec(), false);

            if (bucketCols != null || sortCols != null) {
              updatePartitionBucketSortColumns(db, table, partn, bucketCols,
                  numBuckets, sortCols);
            }

            dc = new DataContainer(table.getTTable(), partn.getTPartition());
            // add this partition to post-execution hook
            if (work.getOutputs() != null) {
              DDLTask.addIfAbsentByName(new WriteEntity(partn,
                getWriteType(tbd, work.getLoadTableWork().getWriteType())), work.getOutputs());
            }
         }
        }
        if (SessionState.get() != null && dc != null) {
          // If we are doing an update or a delete the number of columns in the table will not
          // match the number of columns in the file sink.  For update there will be one too many
          // (because of the ROW__ID), and in the case of the delete there will be just the
          // ROW__ID, which we don't need to worry about from a lineage perspective.
          List<FieldSchema> tableCols = null;
          switch (work.getLoadTableWork().getWriteType()) {
            case DELETE:
            case UPDATE:
              // Pass an empty list as no columns will be written to the file.
              // TODO I should be able to make this work for update
              tableCols = new ArrayList<FieldSchema>();
              break;

            default:
              tableCols = table.getCols();
              break;
          }
          SessionState.get().getLineageState().setLineage(tbd.getSourcePath(), dc, tableCols);
        }
        releaseLocks(tbd);
      }

      return 0;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      setException(e);
      return (1);
    }
  }
  /**
   * so to make sure we crate WriteEntity with the right WriteType.  This is (at this point) only
   * for consistency since LockManager (which is the only thing that pays attention to WriteType)
   * has done it's job before the query ran.
   */
  WriteEntity.WriteType getWriteType(LoadTableDesc tbd, AcidUtils.Operation operation) {
    if(tbd.getReplace()) {
      return WriteEntity.WriteType.INSERT_OVERWRITE;
    }
    switch (operation) {
      case DELETE:
        return WriteEntity.WriteType.DELETE;
      case UPDATE:
        return WriteEntity.WriteType.UPDATE;
      default:
        return WriteEntity.WriteType.INSERT;
    }
  }
  private boolean isSkewedStoredAsDirs(LoadTableDesc tbd) {
    return (tbd.getLbCtx() == null) ? false : tbd.getLbCtx()
        .isSkewedStoredAsDir();
  }

  /**
   * Alters the bucketing and/or sorting columns of the partition provided they meet some
   * validation criteria, e.g. the number of buckets match the number of files, and the
   * columns are not partition columns
   * @param table
   * @param partn
   * @param bucketCols
   * @param numBuckets
   * @param sortCols
   * @throws IOException
   * @throws InvalidOperationException
   * @throws HiveException
   */
  private void updatePartitionBucketSortColumns(Hive db, Table table, Partition partn,
      List<BucketCol> bucketCols, int numBuckets, List<SortCol> sortCols)
          throws IOException, InvalidOperationException, HiveException {

    boolean updateBucketCols = false;
    if (bucketCols != null) {
      FileSystem fileSys = partn.getDataLocation().getFileSystem(conf);
      FileStatus[] fileStatus = HiveStatsUtils.getFileStatusRecurse(
          partn.getDataLocation(), 1, fileSys);
      // Verify the number of buckets equals the number of files
      // This will not hold for dynamic partitions where not every reducer produced a file for
      // those partitions.  In this case the table is not bucketed as Hive requires a files for
      // each bucket.
      if (fileStatus.length == numBuckets) {
        List<String> newBucketCols = new ArrayList<String>();
        updateBucketCols = true;
        for (BucketCol bucketCol : bucketCols) {
          if (bucketCol.getIndexes().get(0) < partn.getCols().size()) {
            newBucketCols.add(partn.getCols().get(
                bucketCol.getIndexes().get(0)).getName());
          } else {
            // If the table is bucketed on a partition column, not valid for bucketing
            updateBucketCols = false;
            break;
          }
        }
        if (updateBucketCols) {
          partn.getBucketCols().clear();
          partn.getBucketCols().addAll(newBucketCols);
          partn.getTPartition().getSd().setNumBuckets(numBuckets);
        }
      }
    }

    boolean updateSortCols = false;
    if (sortCols != null) {
      List<Order> newSortCols = new ArrayList<Order>();
      updateSortCols = true;
      for (SortCol sortCol : sortCols) {
        if (sortCol.getIndexes().get(0) < partn.getCols().size()) {
          newSortCols.add(new Order(
            partn.getCols().get(sortCol.getIndexes().get(0)).getName(),
            sortCol.getSortOrder() == '+' ? BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC :
              BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_DESC));
        } else {
          // If the table is sorted on a partition column, not valid for sorting
          updateSortCols = false;
          break;
        }
      }
      if (updateSortCols) {
        partn.getSortCols().clear();
        partn.getSortCols().addAll(newSortCols);
      }
    }

    if (updateBucketCols || updateSortCols) {
      db.alterPartition(table.getDbName(), table.getTableName(), partn, null);
    }
  }

  /*
   * Does the move task involve moving to a local file system
   */
  public boolean isLocal() {
    LoadTableDesc tbd = work.getLoadTableWork();
    if (tbd != null) {
      return false;
    }

    LoadFileDesc lfd = work.getLoadFileWork();
    if (lfd != null) {
      if (lfd.getIsDfsDir()) {
        return false;
      } else {
        return true;
      }
    }

    return false;
  }

  @Override
  public StageType getType() {
    return StageType.MOVE;
  }

  @Override
  public String getName() {
    return "MOVE";
  }
}
